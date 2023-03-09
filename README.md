# service-candle-writer

docker build -f Dockerfile.valgrind -t candle-writer-valgrind .

##VALGRIND

docker run -it candle-writer-valgrind
valgrind --tool=massif ./target/debug/service-candle-writer
docker cp relaxed_carver:/app/massif.out.8 massif.out.8

 


