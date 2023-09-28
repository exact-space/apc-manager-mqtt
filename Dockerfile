#FROM dev.exactspace.co/python-base-es2:r1
FROM dev.exactspace.co/python3.11-base-es:r1
RUN mkdir /src
COPY *.py /src/
COPY index.py /src/
COPY BUILD_TIME /src/
COPY main /src/
RUN chmod +x /src/main
RUN chmod +x /src/index.py
RUN chmod +x /src/*
WORKDIR /src
ENTRYPOINT ["./main"]
