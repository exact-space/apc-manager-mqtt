#FROM dev.exactspace.co/python-base-es2:r1
FROM dev.exactspace.co/python3.10-base-es:r1
RUN mkdir /src
COPY *.py /src/
COPY index.py /src/
COPY BUILD_TIME /src/
COPY main /src/
RUN chmod +x /src/main
RUN chmod +x /src/index.py
WORKDIR /src
ENTRYPOINT ["./main"]
