# Trend Analysis Microservice

## Architecture Diagram

```mermaid
flowchart TB
    subgraph TrainingData["Training Data Source"]
        direction TB
        TrainingDB[("Training Data Table<br/>---<br/>Historical OHLC Data<br/>for Model Training")]
    end
    
    subgraph Training["Training Phase"]
        direction TB
        HistData["Historical Price Data<br/>(open, close, high, low)"]
        RuleDef["Rule-Based Pattern Definitions<br/>(Pattern Templates)"]
        DataGen["Training Data Generator"]
        TrainData["Labeled Training Dataset<br/>with Hierarchy"]
        Model["ML Model Training"]
        TrainedModel["Trained Model"]
        
        HistData --> DataGen
        RuleDef --> DataGen
        DataGen --> TrainData
        
        TrainData --> |"Hierarchical Labels:<br/>1. Bullish/Bearish<br/>2. Pattern Family<br/>3. Specific Trend"| Model
        Model --> TrainedModel
    end
    
    TrainingDB --> |"Load training data"| HistData
    
    subgraph Application["Application Phase"]
        direction TB
        LiveData["Live Stock Data<br/>(Time Series Windows)"]
        Inference["Model Inference"]
        RawResults["Raw Predictions<br/>with Confidence Scores"]
        Filter["Confidence Filter<br/>(Threshold-based)"]
        FilteredResults["Filtered Results<br/>(High Confidence Only)"]
        
        LiveData --> Inference
        TrainedModel -.->|"Deployed Model"| Inference
        Inference --> RawResults
        RawResults --> Filter
        Filter --> |"e.g., confidence >= 0.7<br/>for Double Top"| FilteredResults
    end
    
    subgraph DataSource["Data Source"]
        direction TB
        StockDB[("Stock Database<br/>---<br/>Historical OHLC Data")]
    end
    
    subgraph Storage["Data Storage"]
        direction TB
        DB[("Database<br/>---<br/>• Pattern Type<br/>• Confidence Score<br/>• Time Interval<br/>• Stock Symbol")]
        FilteredResults --> DB
    end
    
    subgraph Client["Client Application"]
        direction TB
        IncApp["Incrementum App"]
        UserUI["User Interface<br/>(Pattern Visualization)"]
        
        DB --> IncApp
        IncApp --> UserUI
    end
    
    subgraph ContinuousAnalysis["Continuous Analysis Microservice"]
        direction LR
        Scheduler["Pattern Detection<br/>Microservice"]
        StockStream["Stock Data Stream"]
        
        StockStream --> Scheduler
        Scheduler --> |"New patterns detected"| LiveData
    end
    
    StockDB --> |"Retrieve historical data"| ContinuousAnalysis
    
    ContinuousAnalysis -.->|"Continuous Loop"| Application
    
    style TrainingData fill:none,stroke:#fff
    style Training fill:none,stroke:#fff
    style Application fill:none,stroke:#fff
    style DataSource fill:none,stroke:#fff
    style Storage fill:none,stroke:#fff
    style Client fill:none,stroke:#fff
    style ContinuousAnalysis fill:none,stroke:#fff
```

## Pattern Hierarchy

The model is trained to recognize patterns in a hierarchical structure:

1. **Level 1: Market Direction**
   - Bullish (upward trend)
   - Bearish (downward trend)

2. **Level 2: Pattern Family**
   - Reversal patterns
   - Continuation patterns
   - Consolidation patterns

3. **Level 3: Specific Patterns**
   - Double Top
   - Double Bottom
   - Head and Shoulders
   - Triangles
   - Flags and Pennants
   - etc.