#!/usr/bin/env bash

# Copyright 2025 The Kubernetes Authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# This script runs KEP-3926 (AllowUnsafeMalformedObjectDeletion) E2E tests.
#
# Unlike the KMS-based encryption tests (run-e2e.sh), this test:
# 1. Uses simple AESGCM encryption (no KMS plugin needed)
# 2. Creates a Kind cluster with encryption hot-reload enabled
# 3. Runs tests that modify encryption config to simulate corruption
# 4. Tests the IgnoreStoreReadErrorWithClusterBreakingPotential delete option
#
# Usage:
#   ./test/e2e/testing-manifests/auth/encrypt/run-kep3926-e2e.sh
#
# Environment variables:
#   SKIP_DELETE_CLUSTER=true  - Keep cluster after tests for debugging
#   SKIP_RUN_TESTS=true       - Only create cluster, skip test execution
#   SKIP_COLLECT_LOGS=true    - Skip collecting logs and metrics
#   ARTIFACTS                 - Output directory (defaults to _artifacts)

set -o errexit
set -o nounset
set -o pipefail

KUBE_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")"/../../../../.. && pwd -P)"
source "${KUBE_ROOT}/hack/lib/init.sh"

readonly cluster_name="kep3926"

# create_cluster_and_run_test creates a kind cluster using kubetest2 and runs E2E tests.
create_cluster_and_run_test() {
    CLUSTER_CREATE_ATTEMPTED=true

    TEST_ARGS=""
    if [ "${SKIP_RUN_TESTS:-}" != "true" ]; then
        # Run only KEP-3926 corrupt object deletion tests
        # Use --use-built-binaries to use the kubectl, e2e.test, and ginkgo binaries built during --build
        TEST_ARGS="--test=ginkgo -- --focus-regex='Corrupt object deletion' --use-built-binaries"
    else
        echo "Skipping running tests"
    fi

    # shellcheck disable=SC2086
    kubetest2 kind -v 5 \
    --build \
    --up \
    --rundir-in-artifacts \
    --config test/e2e/testing-manifests/auth/encrypt/kind-kep3926.yaml \
    --cluster-name "${cluster_name}" ${TEST_ARGS}
}

cleanup() {
    # CLUSTER_CREATE_ATTEMPTED is true once we run kubetest2 kind --up
    if [ "${CLUSTER_CREATE_ATTEMPTED:-}" = true ]; then
        if [ "${SKIP_COLLECT_LOGS:-}" != "true" ]; then
            # collect logs and metrics
            echo "Collecting logs"
            mkdir -p "${ARTIFACTS}/logs"
            kind "export" logs "${ARTIFACTS}/logs" --name "${cluster_name}" || true

            echo "Collecting metrics"
            mkdir -p "${ARTIFACTS}/metrics"
            kubectl get --raw /metrics > "${ARTIFACTS}/metrics/kube-apiserver-metrics.txt" || true
        else
            echo "Skipping collecting logs and metrics"
        fi

        if [ "${SKIP_DELETE_CLUSTER:-}" != "true" ]; then
            echo "Deleting kind cluster"
            # delete cluster
            kind delete cluster --name "${cluster_name}"
        else
            echo "Skipping deleting kind cluster"
        fi
    fi
}

main(){
    # ensure artifacts (results) directory exists when not in CI
    export ARTIFACTS="${ARTIFACTS:-${PWD}/_artifacts}"
    mkdir -p "${ARTIFACTS}"

    kube::golang::setup_env
    (
        # just while installing external tools
        export GO111MODULE=on GOTOOLCHAIN=auto
        # TODO: consider using specific versions to avoid surprise breaking changes
        go install sigs.k8s.io/kind@latest
        go install sigs.k8s.io/kubetest2@latest
        go install sigs.k8s.io/kubetest2/kubetest2-kind@latest
        go install sigs.k8s.io/kubetest2/kubetest2-tester-ginkgo@latest
    )

    # The build e2e.test, ginkgo and kubectl binaries + copy to dockerized dir is
    # because of https://github.com/kubernetes-sigs/kubetest2/issues/184
    make all WHAT="test/e2e/e2e.test vendor/github.com/onsi/ginkgo/v2/ginkgo cmd/kubectl"
    mkdir -p _output/dockerized/bin/linux/amd64;
    for binary in kubectl e2e.test ginkgo; do
        cp -f _output/local/go/bin/${binary} _output/dockerized/bin/linux/amd64/${binary}
    done;

    create_cluster_and_run_test
    cleanup
}

trap cleanup INT TERM
main "$@"
