#!/usr/bin/env python3.6
#coding:utf-8
import sys
import os
num = len(sys.argv)
if num < 3:
	print("Please input two options")
	sys.exit(1)
arg0 = sys.argv[1]
arg1 = sys.argv[2]
print("第一个参数是:",arg0)
print("第二个参数是:",arg1)
