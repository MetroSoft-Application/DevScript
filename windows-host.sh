#!/bin/bash
WIN_IP=$(/sbin/ip route | awk '/default/ { print $3 }')
HOSTS_FILE="/etc/hosts"
ALIASES="windows-host"

# 既存エントリ削除
sudo sed -i "/$ALIASES/d" $HOSTS_FILE
# 新規追加
echo "$WIN_IP    $ALIASES" | sudo tee -a $HOSTS_FILE > /dev/null