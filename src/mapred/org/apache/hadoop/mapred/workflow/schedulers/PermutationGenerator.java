package org.apache.hadoop.mapred.workflow.schedulers;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

public class PermutationGenerator<T> implements Iterator<List<T>> {

  List<T> types;
  int[] currentPermutation;
  boolean hasNext;

  /**
   * Get all permutations of a given length with replacement from a collection.
   *
   * @param types
   *          A collection of values that each element can be.
   * @param size
   *          The number of elements in / size of the permutation.
   */
  public PermutationGenerator(Collection<T> types, int size) {
    this.types = new ArrayList<T>(types);
    this.hasNext = true;

    // Keep track of the current permutation via indexing into the array of
    // available element types.
    this.currentPermutation = new int[size];
  }

  @Override
  public boolean hasNext() { return hasNext; }

  @Override
  public List<T> next() {
    
    if (!hasNext) { return null; }

    List<T> permutation = new ArrayList<T>();
    for (int i = 0; i < currentPermutation.length; i++) {
      permutation.add(types.get(currentPermutation[i]));
    }

    incrementPermutation(currentPermutation.length - 1);

    return permutation;
  }

  private void incrementPermutation(int digit) {
    
    // We've iterated through all permutations if the next digit to be
    // increased is larger than the most significant digit.
    if (digit < 0) { hasNext = false; return; }

    // Increment a digit. If there is overflow wrt/ number of possible values,
    // then recurse to increase the next most significant digit.
    currentPermutation[digit] = (currentPermutation[digit] + 1) % types.size();
    if (currentPermutation[digit] == 0) { incrementPermutation(digit - 1); }
  }

  @Override
  public void remove() { /* Remove not supported. */ }

}