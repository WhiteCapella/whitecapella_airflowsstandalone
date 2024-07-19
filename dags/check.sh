#!/bin/bash

if [ -f "$HOME/data/signal/{{ds_nodash}}/_DONE ]; then
	exit 0
else
	exit 1
fi

