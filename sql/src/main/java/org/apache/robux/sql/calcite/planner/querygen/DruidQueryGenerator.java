/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.robux.sql.calcite.planner.querygen;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.core.Window;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.apache.robux.error.RobuxException;
import org.apache.robux.query.DataSource;
import org.apache.robux.query.FilteredDataSource;
import org.apache.robux.query.QueryDataSource;
import org.apache.robux.query.UnionDataSource;
import org.apache.robux.query.filter.DimFilter;
import org.apache.robux.sql.calcite.filtration.Filtration;
import org.apache.robux.sql.calcite.planner.PlannerContext;
import org.apache.robux.sql.calcite.planner.querygen.RobuxQueryGenerator.PDQVertexFactory.PDQVertex;
import org.apache.robux.sql.calcite.planner.querygen.SourceDescProducer.SourceDesc;
import org.apache.robux.sql.calcite.rel.RobuxQuery;
import org.apache.robux.sql.calcite.rel.PartialRobuxQuery;
import org.apache.robux.sql.calcite.rel.PartialRobuxQuery.Stage;
import org.apache.robux.sql.calcite.rel.logical.RobuxAggregate;
import org.apache.robux.sql.calcite.rel.logical.RobuxJoin;
import org.apache.robux.sql.calcite.rel.logical.RobuxLogicalNode;
import org.apache.robux.sql.calcite.rel.logical.RobuxSort;
import org.apache.robux.sql.calcite.rel.logical.RobuxUnion;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Stack;

/**
 * Converts a DAG of {@link RobuxLogicalNode} convention to a native {@link RobuxQuery} for execution.
 */
public class RobuxQueryGenerator
{
  private final RobuxLogicalNode relRoot;
  private final PDQVertexFactory vertexFactory;

  public RobuxQueryGenerator(PlannerContext plannerContext, RobuxLogicalNode relRoot, RexBuilder rexBuilder)
  {
    this.relRoot = relRoot;
    this.vertexFactory = new PDQVertexFactory(plannerContext, rexBuilder);
  }

  /**
   * Tracks the upstream nodes during traversal.
   *
   * Its main purpose is to provide access to parent nodes;
   * so that context sensitive logics can be formalized with it.
   */
  public static class RobuxNodeStack
  {
    static class Entry
    {
      public final RobuxLogicalNode node;
      public final int operandIndex;

      public Entry(RobuxLogicalNode node, int operandIndex)
      {
        this.node = node;
        this.operandIndex = operandIndex;
      }

      @Override
      public String toString()
      {
        return ToStringBuilder.reflectionToString(this, ToStringStyle.NO_CLASS_NAME_STYLE);
      }
    }
    Stack<Entry> stack = new Stack<>();
    PlannerContext plannerContext;

    public RobuxNodeStack(PlannerContext plannerContext)
    {
      this.plannerContext = plannerContext;
    }

    public void push(RobuxLogicalNode item)
    {
      push(item, 0);
    }

    public void push(RobuxLogicalNode item, int operandIndex)
    {
      stack.push(new Entry(item, operandIndex));
    }

    public void pop()
    {
      stack.pop();
    }

    public int size()
    {
      return stack.size();
    }

    public RobuxLogicalNode getNode()
    {
      return stack.peek().node;
    }

    public RobuxLogicalNode parentNode()
    {
      return getNode(1).node;
    }

    public Entry getNode(int i)
    {
      return stack.get(stack.size() - 1 - i);
    }

    public int peekOperandIndex()
    {
      return stack.peek().operandIndex;
    }

    public PlannerContext getPlannerContext()
    {
      return plannerContext;
    }
  }

  public RobuxQuery buildQuery()
  {
    RobuxNodeStack stack = new RobuxNodeStack(vertexFactory.plannerContext);
    stack.push(relRoot);
    Vertex vertex = buildVertexFor(stack);
    return vertex.buildQuery(true);
  }

  private Vertex buildVertexFor(RobuxNodeStack stack)
  {
    List<Vertex> newInputs = new ArrayList<>();

    for (RelNode input : stack.getNode().getInputs()) {
      stack.push((RobuxLogicalNode) input, newInputs.size());
      newInputs.add(buildVertexFor(stack));
      stack.pop();
    }
    Vertex vertex = processNodeWithInputs(stack, newInputs);
    return vertex;
  }

  private Vertex processNodeWithInputs(RobuxNodeStack stack, List<Vertex> newInputs)
  {
    RobuxLogicalNode node = stack.getNode();
    if (node instanceof SourceDescProducer) {
      return vertexFactory.createVertex(stack, PartialRobuxQuery.create(node), newInputs);
    }
    if (newInputs.size() == 1) {
      Vertex inputVertex = newInputs.get(0);
      Optional<Vertex> newVertex = inputVertex.extendWith(stack);
      if (newVertex.isPresent()) {
        return newVertex.get();
      }
      inputVertex = vertexFactory.createVertex(
          stack,
          PartialRobuxQuery.createOuterQuery(((PDQVertex) inputVertex).partialRobuxQuery, vertexFactory.plannerContext),
          ImmutableList.of(inputVertex)
      );
      newVertex = inputVertex.extendWith(stack);
      if (newVertex.isPresent()) {
        return newVertex.get();
      }
    }
    throw RobuxException.defensive().build("Unable to process relNode[%s]", node);
  }

  /**
   * Execution dag vertex - encapsulates a list of operators.
   */
  private interface Vertex
  {
    /**
     * Builds the query.
     */
    RobuxQuery buildQuery(boolean isRoot);

    /**
     * Extends the current vertex to include the specified parent.
     */
    Optional<Vertex> extendWith(RobuxNodeStack stack);

    /**
     * Decides wether this {@link Vertex} can be unwrapped into an {@link SourceDesc}.
     */
    boolean canUnwrapSourceDesc();

    /**
     * Unwraps this {@link Vertex} into an {@link SourceDesc}.
     *
     * Unwraps the source of this vertex - if it doesn't do anything beyond reading its input.
     *
     * @throws RobuxException if unwrap is not possible.
     */
    SourceDesc unwrapSourceDesc();
  }

  private static class VertexTweaks
  {
    public final JoinPosition joinType;
    public final boolean isParentUnion;

    public VertexTweaks(JoinPosition joinType, boolean isParentUnion)
    {
      this.joinType = joinType;
      this.isParentUnion = isParentUnion;
    }

    static VertexTweaks analyze(RobuxNodeStack stack)
    {
      JoinPosition joinType = JoinPosition.analyze(stack);
      boolean isParentUnion = stack.size() > 2 && stack.parentNode() instanceof RobuxUnion;
      return new VertexTweaks(joinType, isParentUnion);
    }

    boolean forceSubQuery(SourceDesc sourceDesc)
    {
      if (sourceDesc.dataSource.isGlobal()) {
        return false;
      }
      return joinType == JoinPosition.RIGHT;
    }

    boolean filteredDatasourceAllowed()
    {
      return joinType == JoinPosition.NONE;
    }

    boolean finalizeSubQuery()
    {
      return joinType == JoinPosition.NONE;
    }

    boolean mayUnwrapWithRename()
    {
      return !isParentUnion;
    }

    enum JoinPosition
    {
      NONE, LEFT, RIGHT;

      public static JoinPosition analyze(RobuxNodeStack stack)
      {
        if (stack.size() < 2) {
          return NONE;
        }
        RobuxLogicalNode possibleJoin = stack.parentNode();
        if (!(possibleJoin instanceof RobuxJoin)) {
          return NONE;
        }
        if (stack.peekOperandIndex() == 0) {
          return LEFT;
        } else {
          return RIGHT;
        }
      }
    }
  }

  /**
   * {@link PartialRobuxQuery} based {@link Vertex} factory.
   */
  protected static class PDQVertexFactory
  {
    private final PlannerContext plannerContext;
    private final RexBuilder rexBuilder;

    public PDQVertexFactory(PlannerContext plannerContext, RexBuilder rexBuilder)
    {
      this.plannerContext = plannerContext;
      this.rexBuilder = rexBuilder;
    }

    Vertex createVertex(RobuxNodeStack stack, PartialRobuxQuery partialRobuxQuery, List<Vertex> inputs)
    {
      VertexTweaks tweaks = VertexTweaks.analyze(stack);
      return new PDQVertex(partialRobuxQuery, inputs, tweaks);
    }

    public class PDQVertex implements Vertex
    {
      final PartialRobuxQuery partialRobuxQuery;
      final List<Vertex> inputs;
      final VertexTweaks tweaks;
      private SourceDesc source;

      public PDQVertex(PartialRobuxQuery partialRobuxQuery, List<Vertex> inputs, VertexTweaks tweaks)
      {
        this.partialRobuxQuery = partialRobuxQuery;
        this.inputs = inputs;
        this.tweaks = tweaks;
      }

      @Override
      public RobuxQuery buildQuery(boolean topLevel)
      {
        SourceDesc source = getSource();
        return partialRobuxQuery.build(
            source.dataSource,
            source.rowSignature,
            plannerContext,
            rexBuilder,
            !(topLevel) && tweaks.finalizeSubQuery(),
            true
        );
      }

      private SourceDesc getSource()
      {
        if (source == null) {
          source = realGetSource();
        }
        return source;
      }

      /**
       * Creates the {@link SourceDesc} for the current {@link Vertex}.
       */
      private SourceDesc realGetSource()
      {
        List<SourceDesc> sourceDescs = new ArrayList<>();
        boolean mayUnwrap = mayUnwrapInputs();
        for (Vertex inputVertex : inputs) {
          final SourceDesc desc;
          if (mayUnwrap && inputVertex.canUnwrapSourceDesc()) {
            desc = inputVertex.unwrapSourceDesc();
          } else {
            RobuxQuery inputQuery = inputVertex.buildQuery(false);
            desc = new SourceDesc(new QueryDataSource(inputQuery.getQuery()), inputQuery.getOutputRowSignature());
          }
          sourceDescs.add(desc);
        }
        RelNode scan = partialRobuxQuery.getScan();
        if (scan instanceof SourceDescProducer) {
          SourceDescProducer inp = (SourceDescProducer) scan;
          return inp.getSourceDesc(plannerContext, sourceDescs);
        }
        if (inputs.size() == 1) {
          return sourceDescs.get(0);
        }
        throw RobuxException.defensive("Unable to create SourceDesc for Operator [%s]", scan);
      }

      private boolean mayUnwrapInputs()
      {
        if (!(partialRobuxQuery.getScan() instanceof RobuxUnion)) {
          return true;
        }
        boolean mayUnwrap = true;
        for (Vertex vertex : inputs) {
          if (!vertex.canUnwrapSourceDesc()) {
            mayUnwrap = false;
          }
        }
        return mayUnwrap;
      }

      /**
       * Extends the the current partial query with the new parent if possible.
       */
      @Override
      public Optional<Vertex> extendWith(RobuxNodeStack stack)
      {
        Optional<PartialRobuxQuery> newPartialQuery = extendPartialRobuxQuery(stack);
        if (!newPartialQuery.isPresent()) {
          return Optional.empty();

        }
        return Optional.of(createVertex(stack, newPartialQuery.get(), inputs));
      }

      /**
       * Merges the given {@link RelNode} into the current {@link PartialRobuxQuery}.
       */
      private Optional<PartialRobuxQuery> extendPartialRobuxQuery(RobuxNodeStack stack)
      {
        RobuxLogicalNode parentNode = stack.getNode();
        if (accepts(stack, Stage.WHERE_FILTER, Filter.class)) {
          PartialRobuxQuery newPartialQuery = partialRobuxQuery.withWhereFilter((Filter) parentNode);
          return Optional.of(newPartialQuery);
        }
        if (accepts(stack, Stage.SELECT_PROJECT, Project.class)) {
          PartialRobuxQuery newPartialQuery = partialRobuxQuery.withSelectProject((Project) parentNode);
          return Optional.of(newPartialQuery);
        }
        if (accepts(stack, Stage.AGGREGATE, Aggregate.class)) {
          PartialRobuxQuery newPartialQuery = partialRobuxQuery.withAggregate((Aggregate) parentNode);
          return Optional.of(newPartialQuery);
        }
        if (accepts(stack, Stage.AGGREGATE_PROJECT, Project.class)) {
          PartialRobuxQuery newPartialQuery = partialRobuxQuery.withAggregateProject((Project) parentNode);
          return Optional.of(newPartialQuery);
        }
        if (accepts(stack, Stage.HAVING_FILTER, Filter.class)) {
          PartialRobuxQuery newPartialQuery = partialRobuxQuery.withHavingFilter((Filter) parentNode);
          return Optional.of(newPartialQuery);
        }
        if (accepts(stack, Stage.SORT, Sort.class)) {
          PartialRobuxQuery newPartialQuery = partialRobuxQuery.withSort((Sort) parentNode);
          return Optional.of(newPartialQuery);
        }
        if (accepts(stack, Stage.SORT_PROJECT, Project.class)) {
          PartialRobuxQuery newPartialQuery = partialRobuxQuery.withSortProject((Project) parentNode);
          return Optional.of(newPartialQuery);
        }
        if (accepts(stack, Stage.WINDOW, Window.class)) {
          PartialRobuxQuery newPartialQuery = partialRobuxQuery.withWindow((Window) parentNode);
          return Optional.of(newPartialQuery);
        }
        if (accepts(stack, Stage.WINDOW_PROJECT, Project.class)) {
          PartialRobuxQuery newPartialQuery = partialRobuxQuery.withWindowProject((Project) parentNode);
          return Optional.of(newPartialQuery);
        }
        return Optional.empty();
      }

      private boolean accepts(RobuxNodeStack stack, Stage stage, Class<? extends RelNode> clazz)
      {
        RobuxLogicalNode currentNode = stack.getNode();
        if (Project.class == clazz && stack.size() >= 2) {
          // peek at parent and postpone project for next query stage
          RobuxLogicalNode parentNode = stack.parentNode();
          if (stage.ordinal() > Stage.AGGREGATE.ordinal()
              && parentNode instanceof RobuxAggregate
              && !partialRobuxQuery.canAccept(Stage.AGGREGATE)) {
            return false;
          }
          if (stage.ordinal() > Stage.SORT.ordinal()
              && parentNode instanceof RobuxSort
              && !partialRobuxQuery.canAccept(Stage.SORT)) {
            return false;
          }
        }
        return partialRobuxQuery.canAccept(stage) && clazz.isInstance(currentNode);
      }

      @Override
      public SourceDesc unwrapSourceDesc()
      {
        if (canUnwrapSourceDesc()) {
          RobuxQuery q = buildQuery(false);
          SourceDesc origInput = getSource();
          DataSource dataSource;
          if (q.getFilter() == null) {
            dataSource = origInput.dataSource;
          } else {
            dataSource = makeFilteredDataSource(origInput, q.getFilter());
          }
          return new SourceDesc(dataSource, q.getOutputRowSignature());
        }
        throw RobuxException.defensive("Can't unwrap source of vertex[%s]", partialRobuxQuery);
      }

      @Override
      public boolean canUnwrapSourceDesc()
      {
        if (tweaks.forceSubQuery(getSource())) {
          return false;
        }
        if (partialRobuxQuery.stage() == Stage.SCAN) {
          return true;
        }
        if (tweaks.filteredDatasourceAllowed() && partialRobuxQuery.stage() == PartialRobuxQuery.Stage.WHERE_FILTER) {
          return true;
        }
        if (partialRobuxQuery.stage() == PartialRobuxQuery.Stage.SELECT_PROJECT &&
            (tweaks.filteredDatasourceAllowed() || partialRobuxQuery.getWhereFilter() == null) &&
            mayDiscardSelectProject()) {
          return true;
        }
        return false;
      }

      private boolean mayDiscardSelectProject()
      {
        if (!partialRobuxQuery.getSelectProject().isMapping()) {
          return false;
        }
        if (!tweaks.isParentUnion) {
          return true;
        }
        SourceDesc src = getSource();
        List<String> inputFieldNames = src.rowSignature.getColumnNames();
        List<String> outputFieldNames = partialRobuxQuery.getRowType().getFieldNames();

        if (!isNameConsistentMapping(partialRobuxQuery.getSelectProject(), inputFieldNames, outputFieldNames)) {
          return false;
        }

        boolean isAssociative = UnionDataSource.isCompatibleDataSource(src.dataSource);

        if (!isAssociative) {
          if (!outputFieldNames.equals(inputFieldNames.subList(0, outputFieldNames.size()))) {
            return false;
          }
        }
        return true;
      }

      private boolean isNameConsistentMapping(
          Project selectProject,
          List<String> inputFieldNames,
          List<String> outputFieldNames)
      {
        List<RexNode> projects = selectProject.getProjects();
        for (int i = 0; i < projects.size(); i++) {
          RexInputRef p = (RexInputRef) projects.get(i);
          String inputName = inputFieldNames.get(p.getIndex());
          String outputName = outputFieldNames.get(i);
          if (!inputName.equals(outputName)) {
            return false;
          }
        }
        return true;
      }
    }
  }

  /**
   * This method should not live here.
   *
   * The fact that {@link Filtration} have to be run on the filter is out-of scope here.
   */
  public static FilteredDataSource makeFilteredDataSource(SourceDesc sd, DimFilter filter)
  {

    Filtration filtration = Filtration.create(filter).optimizeFilterOnly(sd.rowSignature);
    DimFilter newFilter = filtration.getDimFilter();
    return FilteredDataSource.create(sd.dataSource, newFilter);
  }
}
