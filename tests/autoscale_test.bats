#!/usr/bin/env bats

setup() {
    echo "setup ${BATS_TEST_NAME} ..." >> ./bats.log
}

teardown() {
    echo "teardown ${BATS_TEST_NAME} ..." >> ./bats.log
}

@test "addition using bc" {
    result="$(echo 2 + 2 | bc)"
    
    [ "${result}" -eq 4 ]
}
