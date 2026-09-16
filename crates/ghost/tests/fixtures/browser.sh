#!/bin/sh
case "$1" in
  --version) printf 'agent-browser 0.35.0\n' ;;
  version) printf '0.1.0\n' ;;
  read) printf 'fixture browser evidence\n' ;;
  close) printf 'closed\n' ;;
  *) exit 1 ;;
esac
