#!/bin/bash

virtualenv venv
source venv/bin/activate
venv/bin/python venv/bin/pip install -r tests/pip-req.txt

# echo ">> test beansdb rw ..."
# export GOBEANSPROXY_TEST_BR=1 GOBEANSPROXY_TEST_BW=1
# export GOBEANSPROXY_TEST_CR=0 GOBEANSPROXY_TEST_CW=0
# venv/bin/python venv/bin/nosetests --with-xunit --xunit-file=unittest.xml

echo ">> test beansdb/cstar dual write, bdb read ..."
export GOBEANSPROXY_TEST_BR=1 GOBEANSPROXY_TEST_BW=1
export GOBEANSPROXY_TEST_CR=0 GOBEANSPROXY_TEST_CW=1
venv/bin/python \
    venv/bin/nosetests \
    --with-xunit -v \
    --xunit-file="unittest-br${GOBEANSPROXY_TEST_BR}-bw${GOBEANSPROXY_TEST_BW}-cr${GOBEANSPROXY_TEST_CR}-cw${GOBEANSPROXY_TEST_CW}.xml"

echo ">> test beansdb/cstar dual write. cstar read ..."
export GOBEANSPROXY_TEST_BR=0 GOBEANSPROXY_TEST_BW=1
export GOBEANSPROXY_TEST_CR=1 GOBEANSPROXY_TEST_CW=1
venv/bin/python \
    venv/bin/nosetests \
    --with-xunit -v \
    --xunit-file="unittest-br${GOBEANSPROXY_TEST_BR}-bw${GOBEANSPROXY_TEST_BW}-cr${GOBEANSPROXY_TEST_CR}-cw${GOBEANSPROXY_TEST_CW}.xml"

echo ">> test cstar rw ..."
export GOBEANSPROXY_TEST_BR=0 GOBEANSPROXY_TEST_BW=0
export GOBEANSPROXY_TEST_CR=1 GOBEANSPROXY_TEST_CW=1
venv/bin/python \
    venv/bin/nosetests \
    --with-xunit -v \
    --xunit-file="unittest-br${GOBEANSPROXY_TEST_BR}-bw${GOBEANSPROXY_TEST_BW}-cr${GOBEANSPROXY_TEST_CR}-cw${GOBEANSPROXY_TEST_CW}.xml"

deactivate
