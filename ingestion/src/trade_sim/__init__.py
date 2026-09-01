"""Trade event simulator and Snowflake loader.

The simulator is not a toy. It is the test harness for the whole pipeline: every
business rule has a corresponding fault that this package can inject on demand, and
each generated batch ships with a manifest of the verdicts the pipeline *should*
reach. `trade-sim reconcile` then compares that manifest against what the pipeline
actually decided, which turns "the demo ran" into "the demo was correct".
"""

__version__ = "1.0.0"
