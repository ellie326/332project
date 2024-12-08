TESTING 할때만 실행 
1. generate input

worker 
sbt assembly 
cd ./332project 
wget https://www.ordinal.com/try.cgi/gensort-linux-1.5.tar.gz
tar -xvf gensort-linux-1.5.tar.gz 
cd 64
./gensort -a 100000 input.txt 

-> 이렇게 하면 input.txt 에 input data 생성 

cd ./sdproject/generateInput 
sbt run 

-> 위의 input.txt 를 같은 directory 의 input 폴더 안에 file1.txt, file2.txt 등등으로 나눠서 저장 

2. sbt assemble for both master and worker
master 
sbt assembly 

worker 
sbt assembly 

3. start sorting 
master
java -jar master.jar [num of worker]

worker 
각 worker machine 에서 (ssh -XYC 2.2.2.101 or SSH -XYC 2.2.2.102 ...) 
java -jar worker.jar 10.2.~~:50051 -I /home/orange/64/input -O /home/orange/[test num]/output_dir

4. combine the output result

worker 
각 worker 내부에서 
cd ./[test num]/output_dir
cat partition* > result 

master 
cd ./332project/sdproject/transfer 
sbt compile 
sbt run [worker 결과들을 저장할 파일 경로] 
"Enter number: " 나오면 "2" enter 
그러면 대기 상태 진입 

worker 
master 에서 나온 "Received Merge Sort Complete Request from ${request.ip}" 의 worker 순서대로 아래 실행 
cd ./332project/sdproject/transfer 
sbt compile 
sbt run 
"Enter number: " 나오면 "1" enter 

5. check the output result
master 
cat result* > final_result 
./valsort -o final.sum ./332project/sdproject/final_result 





