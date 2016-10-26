#!/bin/bash

kill $( jps | awk '/Master|Worker/ { print $1}' )


