# 332project

## Progress Week 1 (10/17~10/20) 

### Progress in the week 
Set up git repository \
Scheduled weekly meeting \
Research on problems of distributed sorting 

### Goal of the next week 
Brainstorming ideas for using AI tech. 
Schedule overall milestones 

### Individual Goal 
- no separate goal yet
- every member individually research on how to design the project and use AI tech. until the next team meeting  


## Progress Week 2 (10/21~10/27)

### Progress in the week 
Define milestones for progress.

#### Milestone
1. Create input and output data.
2. Develop the worker algorithm.
3. Implement worker-master communication.
4. Implement overall sorting.
### Goal of the next week 
Setting up the project environment.
### Individual Goal 
JunHyeok: research worker-master communication methods

JaeWan: research worker algorithm and sorting/partitioning.

JiHyun: research input/output structure


## Progress Week 3 (10/28~11/3)

### Progress in the week

+ [distribued sorting](https://github.com/ellie326/332project/blob/e27bbf1035a94368238c90d0aa1eab1374cde9ce/report/week3_algorithm%20for%20sorting.pdf)
+ [sorting design (AI)](https://github.com/ellie326/332project/blob/57a21db711c9bf9cfb359c10a30065d1f86fb7ab/report/sortingDesign.md)
+ [Communication](https://github.com/ellie326/332project/blob/57a21db711c9bf9cfb359c10a30065d1f86fb7ab/report/Communication.md)

### Goal of the next week

Share above progress,
Create detailed individual plan and start coding 


### Individual goal

Jihyun: research on shuffle algorithm (ppt pg 12) 

JunHyeok: write pseudo code for communication based on Sorting algorithm and data flow.

## Progress Week 4 (11/4~11/10)

### Progress in the week
+ [shuffle alg](https://github.com/ellie326/332project/blob/main/report/shuffle%20algorithm.md)
+ [communication](https://github.com/ellie326/332project/blob/main/report/communication_pseudocode.md)

### Goal of the next week
Implement basic algorithms

### Individual goal
JaeWan: implement sorting inside single disk

JunHyeok: implement gathering information from workers, deciding the key.

JiHyun: implement shuffle algorithm

## Progress Week 5 (11/11~11/17)

### Progress in the week
+ [shuffle pseudocode](https://github.com/ellie326/332project/blob/main/report/ShufflePseudocode.md)
+ [sort-algorithm](https://github.com/ellie326/332project/blob/a92b3d1124c0c2916c7ffbfefb5cfc0c2e169ac3/report/sort-algorithm.md)

### Goal of the next week
Combine sorting algorithms 
Prepare Progress Presentation 

### Individual goal
no individual goal (need to work together) 

## Progress Week 6 (11/18~11/24)

### Progress in the week
+ [internal sorting 수정](https://github.com/ellie326/332project/blob/main/report/distributed%20sorting_edited%20version.pdf) 
- decide which communication method to use -> decided to use "gRPC"
- basic communication part done -> Nov 25th, 00:05 an error detected -> add debugging for the goal of the next week 
- sampling and partitioning on master machine done
- documentation on misunderstanding and feedbacks from the progress presentation session 

### Goal of the next week
- debug communication part 
- Debug on internal sorting and shuffling
- Make test cases 

### Individual goal
JaeWan: make test cases & be ready to combine worker sorting algorithm with communication 

JiHyun: implement shuffling 

## Progress Week 7 (11/25~12/1)

### Progress in the week
- worker, DataProcess, master.scala and etc files coded
- but still working on testing  

### Goal of the next week
- testing & debugging after genSort 

### Individual goal
no Individual goal 
