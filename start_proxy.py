#!/usr/bin/env python3.6
#coding=utf-8
import os
if os.path.isfile('/etc/profile.d/proxy.txt'):
	os.rename('/etc/profile.d/proxy.txt','/etc/profile.d/proxy.sh')
	os.system('source /etc/profile')
