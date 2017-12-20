# Mist Use Cases

There are 3 canonical use cases when Hydrosphere Mist is applicable:

## (1) Batch Prediction Applications
### Technical nature
Applications and services execute parameterized Apache Spark jobs via REST or Messaging API. Assuming that jobs results could not be pre-calculated in advance.

### Examples
Reporting

Simulation (pricing, bank stress testing, taxi rides)

Forecasting (ad campaign, energy savings, others)

Ad-hoc analytics tools for business users (hosted notebooks - web apps for business users)

### Read More
Please check out detailed overview and coding tutorial for [Batch Prediction Applications](/docs/use-cases/enterprise-analytics.md).

## (2) Realtime Machine Learning Applications
### Technical nature
Machine learning models trained in Apache Spark MLLib or other ML framework could be automatically  deployed as a synchronous low latency web services and used for a single row scoring (or serving).
### Examples
Fraud detection

Ad serving

Preventive Maintenance

Recommendations

Artificial Intelligence (realtime decisioning)

NLP, Image Recognition

### Read More
Please check out detailed overview and coding tutorial for [Realtime Machine Learning Applications](/docs/use-cases/ml-realtime.md).

## (3) Reactive Applications
### Technical nature
Apache Spark streaming or long running batch job pushes insights to applications. Applications could parametrize/adjust streaming jobs on the fly.
### Examples
Fraud detection

Preventive Maintenance

Log Analytics

Proactive monitoring

Search (building search index from the filtered stream)

### Read More
Please check out detailed overview and coding tutorial for [Reactive Applications](/docs/use-cases/reactive.md).

## What's next

Learn from use cases and tutorials here:
 - [Enterprise Analytics Applications](/docs/use-cases/enterprise-analytics.md)
 - [Reactive Applications](/docs/use-cases/reactive.md)
 - [Realtime Machine Learning Applications](/docs/use-cases/ml-realtime.md)
