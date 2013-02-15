#!/bin/bash
cat "$1"/*.sql | grep "^INSERT" | sed 's/`/"/g'

