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

import org.apache.druid.error.NotYetImplemented;
import org.apache.druid.msq.input.InputSpec;
import org.apache.druid.msq.input.stage.StageInputSpec;
import org.apache.druid.msq.kernel.StageDefinitionBuilder;
import org.apache.druid.msq.logical.stages.LogicalStage;
import org.apache.druid.msq.querykit.InputNumberDataSource;
import org.apache.druid.query.DataSource;
import org.apache.druid.query.TableDataSource;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.sql.calcite.planner.querygen.SourceDescProducer.SourceDesc;

/**
 * Represents an {@link InputSpec} for {@link LogicalStage}-s.
 */
public abstract class LogicalInputSpec
{
  public abstract InputSpec toInputSpec(StageMaker maker);

  public abstract RowSignature getRowSignature();

  // FIXME ?
  @Deprecated
  public abstract SourceDesc getSourceDesc();

  public static LogicalInputSpec of(LogicalStage inputStage)
  {
    return new DagStageInputSpec(inputStage);
  }

  public static LogicalInputSpec of(InputSpec inputSpec)
  {
    return new PhysicalInputSpec(inputSpec);
  }

  public static LogicalInputSpec broadcast(LogicalStage logicalStage, int inputIndex)
  {
    // FIXME
    // could potentially unwrap LogicalStage if some conditions are met
    // logicalStage.unwrap(InputSpec.class);
    return new BroadcastStageInputSpec(logicalStage,inputIndex);
  }



  static class PhysicalInputSpec extends LogicalInputSpec
  {
    private InputSpec inputSpec;

    public PhysicalInputSpec(InputSpec inputSpec)
    {
      this.inputSpec = inputSpec;
    }

    @Override
    public InputSpec toInputSpec(StageMaker maker)
    {
      return inputSpec;
    }

    @Override
    public RowSignature getRowSignature()
    {
      throw NotYetImplemented.ex(null, "Not supported for this type");
    }

    @Override
    public SourceDesc getSourceDesc()
    {
      throw new RuntimeException("not sure right now!");
    }
  }

  static class DagStageInputSpec extends LogicalInputSpec
  {

    protected LogicalStage inputStage;

    public DagStageInputSpec(LogicalStage inputStage)
    {
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

    //FIXME
    private static final DataSource DUMMY = new TableDataSource("__dummy__");
    @Override
    public SourceDesc getSourceDesc()
    {
      return new SourceDesc(DUMMY, inputStage.getRowSignature());
    }
  }

  static class BroadcastStageInputSpec extends DagStageInputSpec
  {
    private InputNumberDataSource ds;
    private int inputIndex;

    public BroadcastStageInputSpec(LogicalStage inputStage, int inputIndex)
    {
      super(inputStage);
      this.inputIndex = inputIndex;
      ds = new InputNumberDataSource(inputIndex);
    }

    public SourceDesc getSourceDesc()
    {
      return new SourceDesc(ds, inputStage.getRowSignature());
    }

    @Override
    public boolean isBroadcast()
    {
      return true;
    }
  }

  // FIXME reconsider this?
  public boolean isBroadcast() {
    return false;
  }
}
