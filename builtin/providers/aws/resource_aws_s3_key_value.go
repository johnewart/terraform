package aws

import (
	"bytes"
	"fmt"
	"log"
	"strconv"
	"strings"

	"github.com/hashicorp/terraform/helper/hashcode"
	"github.com/hashicorp/terraform/helper/schema"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/s3"
)

func resourceS3KeyMap() *schema.Resource {
	return &schema.Resource{
		Create: resourceS3KeyMapPut,
		Update: resourceS3KeyMapPut,
		Read:   resourceS3KeyMapRead,
		Delete: resourceS3KeyMapDelete,

		Schema: map[string]*schema.Schema{
			"bucket": &schema.Schema{
				Type:     schema.TypeString,
				Required: true,
				ForceNew: true,
			},

			"key": &schema.Schema{
				Type:     schema.TypeSet,
				Optional: true,
				Elem: &schema.Resource{
					Schema: map[string]*schema.Schema{
						"name": &schema.Schema{
							Type:     schema.TypeString,
							Required: true,
						},

						"path": &schema.Schema{
							Type:     schema.TypeString,
							Required: true,
						},

						"value": &schema.Schema{
							Type:     schema.TypeString,
							Optional: true,
							Computed: true,
						},

						"default": &schema.Schema{
							Type:     schema.TypeString,
							Optional: true,
						},

						"delete": &schema.Schema{
							Type:     schema.TypeBool,
							Optional: true,
							Default:  false,
						},
					},
				},
				Set: resourceS3KeysHash,
			},

			"var": &schema.Schema{
				Type:     schema.TypeMap,
				Computed: true,
			},
		},
	}
}

func resourceS3KeysHash(v interface{}) int {
	var buf bytes.Buffer
	m := v.(map[string]interface{})
	buf.WriteString(fmt.Sprintf("%s-", m["name"].(string)))
	buf.WriteString(fmt.Sprintf("%s-", m["path"].(string)))
	return hashcode.String(buf.String())
}

func resourceS3KeyMapPut(d *schema.ResourceData, meta interface{}) error {
	s3conn := meta.(*AWSClient).s3conn

	fmt.Printf("[DEBUG] Updating S3 Key map")

	bucket := d.Get("bucket").(string)
	vars := make(map[string]string)

	keys := d.Get("key").(*schema.Set).List()
	for _, raw := range keys {
		key, s3key, sub, err := parseKey(raw)

		if err != nil {
			return err
		}

		value := sub["value"].(string)
		// Write a value to S3
		if value != "" {
			log.Printf("[DEBUG] Setting key '%s' to '%v' in %s", s3key, value, bucket)
			vars[key] = value
			sub["value"] = value

			body := bytes.NewReader([]byte(value))

			putInput := &s3.PutObjectInput{
				Bucket: aws.String(bucket),
				Key:    aws.String(s3key),
				Body:   body,
			}

			putInput.ContentType = aws.String("text/plain")

			_, err := s3conn.PutObject(putInput)
			if err != nil {
				return fmt.Errorf("Error putting object in S3 bucket (%s): %s", bucket, err)
			}
		} else {
			log.Printf("[DEBUG] Getting key '%s' from %s", s3key, bucket)
			remote_value, err := loadS3Key(s3conn, bucket, s3key)
			if err != nil {
				if awsErr, ok := err.(awserr.RequestFailure); ok && awsErr.StatusCode() == 404 {
					log.Printf("[WARN] Error reading key (%s) -- object not found (404)", s3key)
					remote_value = ""
				} else {
					return fmt.Errorf("Error loading S3 key: %s", err)
				}
			}

			vars[key] = remote_value
			sub["value"] = remote_value
		}
	}

	//d.SetId("s3keys")
	d.Set("key", keys)
	d.Set("var", vars)

	return nil
}

func resourceS3KeyMapRead(d *schema.ResourceData, meta interface{}) error {
	s3conn := meta.(*AWSClient).s3conn

	log.Printf("[DEBUG] Refreshing S3 Keys map")

	bucket := d.Get("bucket").(string)
	vars := make(map[string]string)

	keys := d.Get("key").(*schema.Set).List()
	for _, raw := range keys {
		key, s3key, sub, err := parseKey(raw)

		if err != nil {
			return err
		}

		// Read a value from S3
		log.Printf("[DEBUG] Getting key '%s' from %s", s3key, bucket)
		remote_value, err := loadS3Key(s3conn, bucket, s3key)
		if err != nil {
			if awsErr, ok := err.(awserr.RequestFailure); ok && awsErr.StatusCode() == 404 {
				log.Printf("[WARN] Error reading key (%s) -- object not found (404)", s3key)
				remote_value = defaultValue(sub, key)
			} else {
				return fmt.Errorf("Error loading S3 key: %s", err)
			}
		}

		vars[key] = remote_value
		sub["value"] = remote_value
	}

	d.Set("key", keys)
	d.Set("var", vars)

	return nil
}

func resourceS3KeyMapDelete(d *schema.ResourceData, meta interface{}) error {
	s3conn := meta.(*AWSClient).s3conn

	bucket := d.Get("bucket").(string)
	key := d.Get("key").(string)

	_, err := s3conn.DeleteObject(
		&s3.DeleteObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
		})
	if err != nil {
		return fmt.Errorf("Error deleting S3 bucket object: %s", err)
	}
	return nil
}

// Load a key from a bucket and return the value
func loadS3Key(s3conn *s3.S3, bucket string, key string) (string, error) {

	resp, err := s3conn.GetObject(
		&s3.GetObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
		})

	if err != nil {
		return "", err
	}

	buf := new(bytes.Buffer)
	buf.ReadFrom(resp.Body)
	s := buf.String()
	s = strings.TrimSpace(s)

	fmt.Printf("[DEBUG] Read '%s' from key '%s' in %s", s, key, bucket)

	return s, nil
}

// parseKey is used to parse a key into a name, path, config or error
// stolen from consul_keys.go
func parseKey(raw interface{}) (string, string, map[string]interface{}, error) {
	sub, ok := raw.(map[string]interface{})
	if !ok {
		return "", "", nil, fmt.Errorf("Failed to unroll: %#v", raw)
	}

	key, ok := sub["name"].(string)
	if !ok {
		return "", "", nil, fmt.Errorf("Failed to expand key '%#v'", sub)
	}

	path, ok := sub["path"].(string)
	if !ok {
		return "", "", nil, fmt.Errorf("Failed to get path for key '%s'", key)
	}
	return key, path, sub, nil
}

func defaultValue(sub map[string]interface{}, key string) string {
	// Use a default if given
	if raw, ok := sub["default"]; ok {
		switch def := raw.(type) {
		case string:
			return def
		case bool:
			return strconv.FormatBool(def)
		}
	}

	// No value
	return ""
}
