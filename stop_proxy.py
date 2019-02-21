#!/usr/bin/env python3.6
#coding:utf-8
import os
if os.path.isfile('/etc/profile.d/proxy.sh'):
	os.rename('/etc/profile.d/proxy.sh','/etc/profile.d/proxy.txt')
	os.system('source /etc/profile')
