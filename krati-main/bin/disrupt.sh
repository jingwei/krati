#!/bin/bash

mvn clean test -Dtest=TestSystemExit

for i in {1..1000}
do
  mvn test -Dtest=TestSystemExit
  if [ -d target/test-output ]; then
    du -sh target/test-output
  fi
  echo " "
  echo " "
done
