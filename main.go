package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/cheggaaa/pb/v3"
	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/semaphore"
)

type AccountConfig struct {
	AccessKey string `json:"access_key"`
	SecretKey string `json:"secret_key"`
	Region    string `json:"region"`
	Bucket    string `json:"bucket"`
}

type Config struct {
	Source      AccountConfig `json:"source"`
	Destination AccountConfig `json:"destination"`
	Concurrency int           `json:"concurrency"`
	MaxRetries  int           `json:"max_retries"`
}

func main() {
	configFile := flag.String("config", ".config.json", "Path to configuration file")
	flag.Parse()

	// Read configuration
	data, err := ioutil.ReadFile(*configFile)
	if err != nil {
		log.Fatalf("Failed to read config file: %v", err)
	}

	var cfg Config
	if err := json.Unmarshal(data, &cfg); err != nil {
		log.Fatalf("Failed to parse config: %v", err)
	}

	if cfg.Concurrency <= 0 {
		cfg.Concurrency = 10 // Default concurrency
	}
	if cfg.MaxRetries <= 0 {
		cfg.MaxRetries = 3 // Default retries
	}

	// Initialize AWS clients with retry configuration
	sourceCfg := aws.Config{
		Region:           cfg.Source.Region,
		Credentials:      credentials.NewStaticCredentialsProvider(cfg.Source.AccessKey, cfg.Source.SecretKey, ""),
		RetryMaxAttempts: cfg.MaxRetries,
		RetryMode:        aws.RetryModeStandard,
	}
	sourceClient := s3.NewFromConfig(sourceCfg)

	destCfg := aws.Config{
		Region:           cfg.Destination.Region,
		Credentials:      credentials.NewStaticCredentialsProvider(cfg.Destination.AccessKey, cfg.Destination.SecretKey, ""),
		RetryMaxAttempts: cfg.MaxRetries,
		RetryMode:        aws.RetryModeStandard,
	}
	destClient := s3.NewFromConfig(destCfg)

	ctx := context.Background()

	// Sync bucket configurations
	if err := syncBucketConfig(ctx, sourceClient, destClient, cfg.Source.Bucket, cfg.Destination.Bucket); err != nil {
		log.Fatalf("Failed to sync bucket configurations: %v", err)
	}

	// List all objects in source bucket
	var objects []types.Object
	listInput := &s3.ListObjectsV2Input{
		Bucket: aws.String(cfg.Source.Bucket),
	}
	for {
		output, err := sourceClient.ListObjectsV2(ctx, listInput)
		if err != nil {
			log.Fatalf("Failed to list source objects: %v", err)
		}
		objects = append(objects, output.Contents...)
		if output.NextContinuationToken == nil || *output.NextContinuationToken == "" {
			break
		}
		listInput.ContinuationToken = output.NextContinuationToken
	}

	log.Printf("Found %d objects in source bucket", len(objects))

	// Initialize progress bar
	bar := pb.StartNew(len(objects))
	bar.SetTemplateString(`{{string . "prefix" | printf "%-20s"}} {{counters . }} {{percent .}} {{etime .}}`)
	bar.Set("prefix", "Syncing objects:")

	// Use errgroup and semaphore for concurrent copying
	g, ctx := errgroup.WithContext(ctx)
	sem := semaphore.NewWeighted(int64(cfg.Concurrency))
	var mu sync.Mutex
	var skipped, copied int

	for _, obj := range objects {
		key := *obj.Key
		g.Go(func() error {
			if err := sem.Acquire(ctx, 1); err != nil {
				return fmt.Errorf("failed to acquire semaphore for %s: %w", key, err)
			}
			defer sem.Release(1)

			// Check if object needs to be copied (incremental check)
			headInput := &s3.HeadObjectInput{
				Bucket: aws.String(cfg.Destination.Bucket),
				Key:    aws.String(key),
			}
			headOutput, err := destClient.HeadObject(ctx, headInput)
			if err != nil {
				if strings.Contains(err.Error(), "NotFound") || strings.Contains(err.Error(), "404") {
					// Object not found in destination, copy it
					if err := copyAndVerifyObject(ctx, sourceClient, destClient, cfg.Source.Bucket, cfg.Destination.Bucket, key, *obj.Size, *obj.ETag); err != nil {
						return err
					}
					mu.Lock()
					copied++
					bar.Increment()
					mu.Unlock()
					return nil
				}
				return fmt.Errorf("failed to head destination object %s: %w", key, err)
			}

			// Compare Size and ETag for changes
			if *headOutput.ContentLength != *obj.Size || *headOutput.ETag != *obj.ETag {
				// Object changed, copy it
				if err := copyAndVerifyObject(ctx, sourceClient, destClient, cfg.Source.Bucket, cfg.Destination.Bucket, key, *obj.Size, *obj.ETag); err != nil {
					return err
				}
				mu.Lock()
				copied++
				bar.Increment()
				mu.Unlock()
			} else {
				log.Printf("Object %s is up-to-date, skipping", key)
				mu.Lock()
				skipped++
				bar.Increment()
				mu.Unlock()
			}

			return nil
		})
	}

	if err := g.Wait(); err != nil {
		bar.Finish()
		log.Fatalf("Error during synchronization: %v", err)
	}

	bar.Finish()
	log.Printf("Synchronization completed: %d objects copied, %d objects skipped", copied, skipped)
}

func syncBucketConfig(ctx context.Context, sourceClient, destClient *s3.Client, sourceBucket, destBucket string) error {
	// Create destination bucket if it doesn't exist
	_, err := destClient.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket: aws.String(destBucket),
	})
	if err != nil && !strings.Contains(err.Error(), "BucketAlreadyOwnedByYou") && !strings.Contains(err.Error(), "BucketAlreadyExists") {
		return fmt.Errorf("failed to create destination bucket %s: %w", destBucket, err)
	}
	log.Printf("Ensured destination bucket %s exists", destBucket)

	// Sync bucket policy
	policyOutput, err := sourceClient.GetBucketPolicy(ctx, &s3.GetBucketPolicyInput{
		Bucket: aws.String(sourceBucket),
	})
	if err == nil && policyOutput.Policy != nil {
		_, err = destClient.PutBucketPolicy(ctx, &s3.PutBucketPolicyInput{
			Bucket: aws.String(destBucket),
			Policy: policyOutput.Policy,
		})
		if err != nil {
			return fmt.Errorf("failed to sync bucket policy for %s: %w", destBucket, err)
		}
		log.Println("Synced bucket policy")
	} else if !strings.Contains(err.Error(), "NoSuchBucketPolicy") {
		return fmt.Errorf("failed to get source bucket policy for %s: %w", sourceBucket, err)
	}

	// Sync bucket versioning
	versioningOutput, err := sourceClient.GetBucketVersioning(ctx, &s3.GetBucketVersioningInput{
		Bucket: aws.String(sourceBucket),
	})
	if err != nil {
		return fmt.Errorf("failed to get source bucket versioning for %s: %w", sourceBucket, err)
	}
	_, err = destClient.PutBucketVersioning(ctx, &s3.PutBucketVersioningInput{
		Bucket: aws.String(destBucket),
		VersioningConfiguration: &types.VersioningConfiguration{
			Status: versioningOutput.Status,
		},
	})
	if err != nil {
		return fmt.Errorf("failed to sync bucket versioning for %s: %w", destBucket, err)
	}
	log.Println("Synced bucket versioning")

	// Sync lifecycle rules
	lifecycleOutput, err := sourceClient.GetBucketLifecycleConfiguration(ctx, &s3.GetBucketLifecycleConfigurationInput{
		Bucket: aws.String(sourceBucket),
	})
	if err == nil && len(lifecycleOutput.Rules) > 0 {
		_, err = destClient.PutBucketLifecycleConfiguration(ctx, &s3.PutBucketLifecycleConfigurationInput{
			Bucket: aws.String(destBucket),
			LifecycleConfiguration: &types.BucketLifecycleConfiguration{
				Rules: lifecycleOutput.Rules,
			},
		})
		if err != nil {
			return fmt.Errorf("failed to sync lifecycle rules for %s: %w", destBucket, err)
		}
		log.Println("Synced lifecycle rules")
	} else if !strings.Contains(err.Error(), "NoSuchLifecycleConfiguration") {
		return fmt.Errorf("failed to get source lifecycle configuration for %s: %w", sourceBucket, err)
	}

	return nil
}

func copyAndVerifyObject(ctx context.Context, sourceClient, destClient *s3.Client, sourceBucket, destBucket, key string, sourceSize int64, sourceETag string) error {
	// Get source object metadata and tags
	headInput := &s3.HeadObjectInput{
		Bucket: aws.String(sourceBucket),
		Key:    aws.String(key),
	}
	headOutput, err := sourceClient.HeadObject(ctx, headInput)
	if err != nil {
		return fmt.Errorf("failed to head source object %s: %w", key, err)
	}

	// Get source object tags
	tagsOutput, err := sourceClient.GetObjectTagging(ctx, &s3.GetObjectTaggingInput{
		Bucket: aws.String(sourceBucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return fmt.Errorf("failed to get source object tags for %s: %w", key, err)
	}

	// Copy object with metadata and tags
	copySource := sourceBucket + "/" + key
	copyInput := &s3.CopyObjectInput{
		Bucket:            aws.String(destBucket),
		CopySource:        aws.String(copySource),
		Key:               aws.String(key),
		MetadataDirective: types.MetadataDirectiveCopy,
		TaggingDirective:  types.TaggingDirectiveCopy,
		StorageClass:      headOutput.StorageClass,
		Metadata:          headOutput.Metadata,
	}
	_, err = destClient.CopyObject(ctx, copyInput)
	if err != nil {
		return fmt.Errorf("failed to copy object %s: %w", key, err)
	}

	// Verify copied object
	verifyInput := &s3.HeadObjectInput{
		Bucket: aws.String(destBucket),
		Key:    aws.String(key),
	}
	verifyOutput, err := destClient.HeadObject(ctx, verifyInput)
	if err != nil {
		return fmt.Errorf("failed to verify copied object %s: %w", key, err)
	}

	// Check Size and ETag
	if *verifyOutput.ContentLength != sourceSize || *verifyOutput.ETag != sourceETag {
		return fmt.Errorf("verification failed for object %s: size (source: %d, dest: %d), ETag (source: %s, dest: %s)",
			key, sourceSize, *verifyOutput.ContentLength, sourceETag, *verifyOutput.ETag)
	}

	log.Printf("Copied and verified object %s with metadata and tags", key)
	return nil
}