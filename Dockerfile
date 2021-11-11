FROM debian:stable-slim
RUN apt-get update && apt-get install iperf

CMD iperf -s
