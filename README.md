Robert Reiners
Project 3

<h2>Question 1</h2>
|        Graph file       |           MIS file           | Is an MIS? |
| ----------------------- | ---------------------------- | ---------- |
| small_edges.csv         | small_edges_MIS.csv          | Yes        |
| small_edges.csv         | small_edges_non_MIS.csv      | No         |
| line_100_edges.csv      | line_100_MIS_test_1.csv      | Yes        |
| line_100_edges.csv      | line_100_MIS_test_2.csv      | No         |
| twitter_10000_edges.csv | twitter_10000_MIS_test_1.csv | Yes        |
| twitter_10000_edges.csv | twitter_10000_MIS_test_2.csv | Yes        |

<h2>Question 2</h2>
I was unable to get Luby's algorithm to work. I ran into problems trying to save the updated values to the graph for output. I have included my attempt commented out. 

<h2>Question 3</h2>
For this problem, since I could not get the algorithm working I will explain what I would expect to find. Because the original twitter graph is very large, having more cores would outweigh the cost of splitting the data. Because of this, 3x4 would be fastest, followed by 4x2 and 2x2 respectively. Increasing the number of nodes would result in latency due to the communication of the nodes, but again because the dataset is very large this would be outweighed by the increased number of cores. Taking the 2x2 configuration as taking n time, we would expect the 4x2 to take n/2 time, and 3x4 to take n/3 time.