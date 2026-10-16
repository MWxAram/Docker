#!/bin/sh
# Заранее скачиваем образы, иначе первый запуск каждого языка будет долгим
for img in python:3.12-slim node:22-slim eclipse-temurin:21-jdk gcc:14 \
           golang:1.23 rust:1-slim ruby:3.3-slim php:8.3-cli bash:5; do
  docker pull "$img"
done
