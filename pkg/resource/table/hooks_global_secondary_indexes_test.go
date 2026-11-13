package table

import (
	"reflect"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	svcsdktypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/aws-controllers-k8s/dynamodb-controller/apis/v1alpha1"
)

func Test_newSDKProvisionedThroughput(t *testing.T) {
	type args struct {
		pt *v1alpha1.ProvisionedThroughput
	}
	tests := []struct {
		name string
		args args
		want *svcsdktypes.ProvisionedThroughput
	}{
		{
			name: "provisioned throughput is  nil",
			args: args{
				pt: nil,
			},
			want: nil,
		},
		{
			name: "provisioned throughput is not nil, read capacity units is nil",
			args: args{
				pt: &v1alpha1.ProvisionedThroughput{
					ReadCapacityUnits:  nil,
					WriteCapacityUnits: aws.Int64(10),
				},
			},
			want: &svcsdktypes.ProvisionedThroughput{
				ReadCapacityUnits:  aws.Int64(1),
				WriteCapacityUnits: aws.Int64(10),
			},
		},
		{
			name: "provisioned throughput is not nil, write capacity units is nil",
			args: args{
				pt: &v1alpha1.ProvisionedThroughput{
					ReadCapacityUnits:  aws.Int64(10),
					WriteCapacityUnits: nil,
				},
			},
			want: &svcsdktypes.ProvisionedThroughput{
				ReadCapacityUnits:  aws.Int64(10),
				WriteCapacityUnits: aws.Int64(1),
			},
		},
		{
			name: "provisioned throughput is not nil, write and read capacity units are not nil",
			args: args{
				pt: &v1alpha1.ProvisionedThroughput{
					ReadCapacityUnits:  aws.Int64(5),
					WriteCapacityUnits: aws.Int64(5),
				},
			},
			want: &svcsdktypes.ProvisionedThroughput{
				ReadCapacityUnits:  aws.Int64(5),
				WriteCapacityUnits: aws.Int64(5),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := newSDKProvisionedThroughput(tt.args.pt); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("newSDKProvisionedThroughput() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_newSDKWarmThroughput(t *testing.T) {
	tests := []struct {
		name string
		wt   *v1alpha1.WarmThroughput
		want *svcsdktypes.WarmThroughput
	}{
		{
			name: "nil returns nil",
			wt:   nil,
			want: nil,
		},
		{
			name: "both fields set",
			wt: &v1alpha1.WarmThroughput{
				ReadUnitsPerSecond:  aws.Int64(13000),
				WriteUnitsPerSecond: aws.Int64(5000),
			},
			want: &svcsdktypes.WarmThroughput{
				ReadUnitsPerSecond:  aws.Int64(13000),
				WriteUnitsPerSecond: aws.Int64(5000),
			},
		},
		{
			name: "read only",
			wt: &v1alpha1.WarmThroughput{
				ReadUnitsPerSecond: aws.Int64(13000),
			},
			want: &svcsdktypes.WarmThroughput{
				ReadUnitsPerSecond: aws.Int64(13000),
			},
		},
		{
			name: "both nil fields",
			wt:   &v1alpha1.WarmThroughput{},
			want: &svcsdktypes.WarmThroughput{},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := newSDKWarmThroughput(tt.wt); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("newSDKWarmThroughput() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_canUpdateTableGSIs(t *testing.T) {
	gsi := func(indexStatus string, wtStatus *string) *v1alpha1.GlobalSecondaryIndexDescription {
		d := &v1alpha1.GlobalSecondaryIndexDescription{
			IndexName:   aws.String("gsi1"),
			IndexStatus: aws.String(indexStatus),
		}
		if wtStatus != nil {
			d.WarmThroughput = &v1alpha1.GlobalSecondaryIndexWarmThroughputDescription{
				ReadUnitsPerSecond:  aws.Int64(13000),
				WriteUnitsPerSecond: aws.Int64(5000),
				Status:              wtStatus,
			}
		}
		return d
	}
	tests := []struct {
		name string
		gsis []*v1alpha1.GlobalSecondaryIndexDescription
		want bool
	}{
		{
			name: "all ACTIVE with nil warm throughput",
			gsis: []*v1alpha1.GlobalSecondaryIndexDescription{gsi("ACTIVE", nil)},
			want: true,
		},
		{
			name: "ACTIVE with warm throughput ACTIVE",
			gsis: []*v1alpha1.GlobalSecondaryIndexDescription{gsi("ACTIVE", aws.String("ACTIVE"))},
			want: true,
		},
		{
			name: "ACTIVE with warm throughput UPDATING",
			gsis: []*v1alpha1.GlobalSecondaryIndexDescription{gsi("ACTIVE", aws.String("UPDATING"))},
			want: false,
		},
		{
			name: "index status UPDATING",
			gsis: []*v1alpha1.GlobalSecondaryIndexDescription{gsi("UPDATING", nil)},
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := &resource{ko: &v1alpha1.Table{
				Status: v1alpha1.TableStatus{GlobalSecondaryIndexesDescriptions: tt.gsis},
			}}
			if got := canUpdateTableGSIs(r); got != tt.want {
				t.Errorf("canUpdateTableGSIs() = %v, want %v", got, tt.want)
			}
		})
	}
}
