# Makefile for DistributedSystems project without Maven
# Compiles into ./bin

SRC_DIRS = Shared/src/main/java AggregationServer/src/main/java ContentServer/src/main/java GetClient/src/main/java
BIN_DIR = bin

JAVAC = javac
JAVA = java
JFLAGS = -d $(BIN_DIR) -cp $(BIN_DIR)

# Main classes
AGGREGATION_MAIN = com.distributedsystems.aggregationserver.AggregationServer
CONTENT_MAIN     = com.distributedsystems.contentserver.ContentServer
GETCLIENT_MAIN   = com.distributedsystems.getclient.GetClient

.PHONY: all clean run-aggregation run-content run-client

all: $(BIN_DIR)/.compiled

# Compile everything
$(BIN_DIR)/.compiled:
	mkdir -p $(BIN_DIR)
	$(JAVAC) $(JFLAGS) $(shell find $(SRC_DIRS) -name "*.java")
	touch $(BIN_DIR)/.compiled

# Run AggregationServer
run-aggregation: all
	$(JAVA) -cp $(BIN_DIR) $(AGGREGATION_MAIN) $(ARGS)

# Run ContentServer
run-content: all
	$(JAVA) -cp $(BIN_DIR) $(CONTENT_MAIN) $(ARGS)

# Run GetClient
run-client: all
	$(JAVA) -cp $(BIN_DIR) $(GETCLIENT_MAIN) $(ARGS)

# Clean build artifacts
clean:
	rm -rf $(BIN_DIR)
