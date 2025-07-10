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

package org.apache.druid.msq.logical;

import org.apache.druid.msq.input.InputSpec;
import org.apache.druid.msq.input.stage.StageInputSpec;
import org.apache.druid.msq.kernel.StageDefinitionBuilder;
import org.apache.druid.msq.logical.stages.LogicalStage;
import org.apache.druid.msq.querykit.InputNumberDataSource;
import org.apache.druid.query.DataSource;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.sql.calcite.planner.querygen.SourceDescProducer.SourceDesc;

/**
 * Represents an {@link InputSpec} for {@link LogicalStage}-s.
 */
public abstract class LogicalInputSpec
{
  public interface InputProperty
  {
    InputProperty BROADCAST = new Broadcast();
  }

  static final class Broadcast implements InputProperty
  {
    private Broadcast()
    {
    }
  }

  final int inputIndex;
  final InputProperty[] props;

  public LogicalInputSpec(int inputIndex, InputProperty[] props)
  {
    this.inputIndex = inputIndex;
    this.props = props;
  }

  public abstract InputSpec toInputSpec(StageMaker maker);

  public abstract RowSignature getRowSignature();

  /**
   * Supplied to make it more interoperable with {@link DataSource} backed
   * features still in use.
   */
  public final SourceDesc getSourceDesc()
  {
    InputNumberDataSource ds = new InputNumberDataSource(inputIndex);
    return new SourceDesc(ds, getRowSignature());
  }

  public final boolean hasProperty(InputProperty prop)
  {
    for (InputProperty p : props) {
      if (p == prop) {
        return true;
      }
    }
    return false;
  }

  public static LogicalInputSpec of(LogicalStage inputStage)
  {
    return of(inputStage, 0);
  }

  public static LogicalInputSpec of(InputSpec inputSpec, RowSignature rowSignature)
  {
    return new PhysicalInputSpec(inputSpec, 0, rowSignature);
  }

  public static LogicalInputSpec of(LogicalStage logicalStage, int inputIndex, InputProperty... props)
  {
    // FIXME
    // could potentially unwrap LogicalStage if some conditions are met
    // logicalStage.unwrap(InputSpec.class);

    return new DagStageInputSpec(logicalStage, inputIndex, props);
  }

  static class PhysicalInputSpec extends LogicalInputSpec
  {
    private InputSpec inputSpec;
    private RowSignature rowSignature;

    public PhysicalInputSpec(InputSpec inputSpec, int inputIndex, RowSignature rowSignature, InputProperty... props)
    {
      super(inputIndex, props);
      this.inputSpec = inputSpec;
      this.rowSignature = rowSignature;
    }

    @Override
    public InputSpec toInputSpec(StageMaker maker)
    {
      return inputSpec;
    }

    @Override
    public RowSignature getRowSignature()
    {
      return rowSignature;
    }
  }

  static class DagStageInputSpec extends LogicalInputSpec
  {
    protected LogicalStage inputStage;

    public DagStageInputSpec(LogicalStage inputStage, int inputIndex, InputProperty[] props)
    {
      super(inputIndex, props);
      this.inputStage = inputStage;
    }

    @Override
    public InputSpec toInputSpec(StageMaker maker)
    {
      StageDefinitionBuilder stage = maker.buildStage(inputStage);
      return new StageInputSpec(stage.getStageNumber());
    }

    public LogicalStage getStage()
    {
      return inputStage;
    }

    @Override
    public RowSignature getRowSignature()
    {
      return inputStage.getLogicalRowSignature();
    }
  }
}
