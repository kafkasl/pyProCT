#!/bin/bash

extraeDir=/home/kurtz/xlin/tfg/lib/extrae_custom/

slot=$1
val=$2

$extraeDir/src/cmd-line/extrae-cmd emit $slot 8000000 $val
