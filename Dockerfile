FROM python:3.11-bookworm as builder

RUN apt-get update && apt-get -y upgrade && mkdir  -p /app/ && python3 -m venv /app/.venv 
RUN apt-get install -y cmake
RUN rm /bin/sh && ln -s /bin/bash /bin/sh
COPY requirements.txt /app/
RUN cd app && source .venv/bin/activate && pip3 install -r requirements.txt 

FROM python:3.11-bookworm as runtime-image
RUN adduser opencost
COPY --from=builder /app /app 
COPY src/opencost_parquet_exporter.py /app/opencost_parquet_exporter.py
RUN chmod 755 /app/opencost_parquet_exporter.py && chown -R opencost /app/  
USER opencost
ENV PATH="/app/.venv/bin:$PATH"
CMD ["/app/opencost_parquet_exporter.py"]
ENTRYPOINT ["/app/.venv/bin/python3"]
