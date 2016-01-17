package nicetools

import (
	"strconv"

	"github.com/aws/aws-sdk-go/aws"
)

// AwsIntString converts int64 value to AWS SDK string pointer.
func AwsIntString(v int64) *string {
	return aws.String(strconv.FormatInt(v, 10))
}
