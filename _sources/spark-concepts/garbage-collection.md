## Garbage Collection

Within the Spark UI you might have come across something called GC time. Time spent on Garbage Collection (GC) is not time well spent, if a large chunk of the total processing time is made up of GC time you might want to make small adjustments to your `SparkSession`.

```{figure} /images/gc_fig1.PNG
---
width: 75%
name: gc_1 
alt: Figure that shows sequential allocation of memory in Spark executors.
---
Sequential executor memory allocation before modifying/altering the data set.
```

To explain what this is, imagine a 2D grid that represents the executor memory, similar to the above figure. Blocks of data are stored on the grid in an orderly manner, say from top-left to bottom-right, and new blocks are added adjacent to the previous block that were written. As we carry out our processing we will delete the odd block here and there and our grid will have lots of holes in it. There will then be a point where we are running out of space on the bottom-right of on our grid, but there are lots of free space dotted around the grid. An example is shown in the figure below. 

```{figure} /images/gc_fig2.PNG
---
width: 75% 
name: gc_2
alt: Figure showing empty gaps in memory in seemingly random locations if deleting or removing data from a distributed data set.
---
Arrangement of data in executor memory after deleting or removing sections of data.
```


At this point a *garbage collection* routine will take place, which will rewrite all the data in our grid from top-left to bottom-right to get rid of the holes and make more space on the bottom-right of the grid, as shown in the figure below. Garbage collection is called a *"Stop the World Event"*, which means when it needs doing all other processing will stop and wait for it to finish.


```{figure} /images/gc_fig3.PNG
---
width: 75% 
name: gc_3
alt: A re-ordered block of data with the gaps removed after the Garbage collection routine has completed and removed the gaps in the data.
---
Re-ordered executor memory after Garbage Collection routine has completed. 
```

If you see that lots of time is spent on GC time, first consider whether the code can be written in a more efficient manner. If not, try increasing the executor memory whilst keeping the overall memory allocation constant, i.e. a smaller number of bulkier executors. In our analogy, this will result in larger grids and possibly avoid as many costly GC processes.

### Further Resources

Spark at the ONS Articles:
- [Spark Application and UI](../spark-concepts/spark-application-and-ui)