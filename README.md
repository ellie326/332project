### BUILD 
먼저, worker maching 과 master machine 에서 `sbt assembly` 명령어를 입력하여 build 를 해준다. 


### TESTING 할때만 실행 (gensort 사용 방법) 
worker machine 에서 아래의 명령어를 순서대로 실행시키고 나면 .../64/input.txt 라는 파일이 생성된다. 

```
cd ~ 
wget https://www.ordinal.com/try.cgi/gensort-linux-1.5.tar.gz
tar -xvf gensort-linux-1.5.tar.gz 
cd 64
./gensort -a 100000 input.txt 
```

그리고 generateInput 폴더의 main 함수를 실행하여 위에서 생성한 input.txt 를 file1.txt, file2.txt 등으로 잘라서 input 이라는 폴더에 저장해준다. 
```
cd ./sdproject/generateInput 
sbt run 
```

### EXECUTION 
먼저 master 머신을 실행시켜야 한다. 아래의 명령어를 입력하여 master machine 이 입력을 대기받도록 한다. 

```
java -jar master.jar [num of worker]
```

그리고 각 worker machine 을 실행시키기 위해서 아래의 명령어를 한 문장씩 입력한다. 
```
ssh -XYC [worker ip]
java -jar worker.jar 10.1.25.21:50051 -I [input data directory1] [input data directory2] ... -O [output data directory] 
```

### Checking (intra machine checking) 
(만약 gensort 를 하지 않았다면 아래의 명령어 실행 필요 -- valsort 함수를 실행시키기 위함) 
```
cd ~ 
wget https://www.ordinal.com/try.cgi/gensort-linux-1.5.tar.gz
tar -xvf gensort-linux-1.5.tar.gz
```

각 worker 내부에서 저장한 partition 값 들을 합쳐서 valsort 를 사용해 확인해주었다. 아래의 명령어를 순서대로 실행한다. 

```
cd [output data directory/output_dir/]
cat partition* >result
cd ~
./64/valsort -o result.sum [output data directory/result] 
```
