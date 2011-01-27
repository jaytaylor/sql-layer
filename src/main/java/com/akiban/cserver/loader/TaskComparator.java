package com.akiban.cserver.loader;

import java.util.Comparator;

public class TaskComparator implements Comparator<Task>
{
	public int compare(Task task1, Task task2)
	{
		Integer depth1 = task1.table().getDepth();
		Integer depth2 = task2.table().getDepth();
		return depth1.compareTo(depth2);
	}

}
