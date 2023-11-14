package table

import (
	"reflect"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	svcsdk "github.com/aws/aws-sdk-go/service/dynamodb"

	"github.com/aws-controllers-k8s/dynamodb-controller/apis/v1alpha1"
)

func Test_newSDKProvisionedThroughput(t *testing.T) {
	type args struct {
		pt *v1alpha1.ProvisionedThroughput
	}
	tests := []struct {
		name string
		args args
		want *svcsdk.ProvisionedThroughput
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
			want: &svcsdk.ProvisionedThroughput{
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
			want: &svcsdk.ProvisionedThroughput{
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
			want: &svcsdk.ProvisionedThroughput{
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
