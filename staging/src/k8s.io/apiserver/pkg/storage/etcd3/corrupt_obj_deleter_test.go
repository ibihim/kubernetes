/*
Copyright The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package etcd3

import (
	"context"
	"errors"
	"testing"

	"k8s.io/apimachinery/pkg/runtime"
	utilerrors "k8s.io/apimachinery/pkg/util/errors"
)

func TestAggregatedStorageErrorMessage(t *testing.T) {
	tests := []struct {
		name     string
		abortErr error
		want     string
	}{
		{
			name: "aggregation completed without an abort",
			want: "unable to transform or decode 2 objects: {\n\tfoo is corrupt\n\tbar is corrupt\n}",
		},
		{
			name:     "aggregation aborted after reaching the limit",
			abortErr: errTooMany,
			want:     "unable to transform or decode 2 objects: {\n\tfoo is corrupt\n\tbar is corrupt\n}, aborted: too many errors, the list is truncated",
		},
		{
			name:     "aggregation aborted due to an unexpected error",
			abortErr: errors.New("etcd is down"),
			want:     "unable to transform or decode 2 objects: {\n\tfoo is corrupt\n\tbar is corrupt\n}, aborted: etcd is down",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := &aggregatedStorageError{
				resourcePrefix: "list",
				errs:           utilerrors.NewAggregate([]error{errors.New("foo is corrupt"), errors.New("bar is corrupt")}),
				abortErr:       test.abortErr,
			}
			if want, got := test.want, err.Error(); want != got {
				t.Errorf("unexpected error message, want:\n%s\ngot:\n%s", want, got)
			}
		})
	}
}

type fakeDecoder struct {
	err error
}

func (d *fakeDecoder) Decode(value []byte, objPtr runtime.Object, rev int64) error {
	return d.err
}

func (d *fakeDecoder) DecodeListItem(ctx context.Context, data []byte, rev uint64, newItemFunc func() runtime.Object) (runtime.Object, error) {
	return nil, d.err
}

func TestCorruptObjErrorInterpretingDecoder(t *testing.T) {
	tests := []struct {
		name        string
		err         error
		wantCorrupt bool
	}{
		{
			name: "no error",
		},
		{
			name:        "decode error is deemed a corrupt object",
			err:         errors.New("object not decodable"),
			wantCorrupt: true,
		},
		{
			name: "conversion failure is not deemed a corrupt object",
			err:  runtime.NewConversionFailedError(errors.New("conversion webhook failed")),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			decoder := WithCorruptObjErrorHandlingDecoder(&fakeDecoder{err: test.err})

			verify := func(t *testing.T, got error) {
				t.Helper()
				var corruptObjErr *corruptObjectError
				if want, isCorrupt := test.wantCorrupt, errors.As(got, &corruptObjErr); want != isCorrupt {
					t.Errorf("corrupt object error: want %t, got %t, error: %v", want, isCorrupt, got)
				}
				if !test.wantCorrupt && !errors.Is(got, test.err) {
					t.Errorf("expected the error to be returned unchanged, want: %v, got: %v", test.err, got)
				}
			}

			verify(t, decoder.Decode(nil, nil, 1))
			_, err := decoder.DecodeListItem(context.Background(), nil, 1, nil)
			verify(t, err)
		})
	}
}
