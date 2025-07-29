#!/bin/bash

# Test runner script for Kafka Spark Streaming project
# This script runs different types of tests and generates coverage reports

set -e

echo "=== Kafka Spark Streaming Test Suite ==="
echo

# Function to check if Docker is running
check_docker() {
    if ! docker info > /dev/null 2>&1; then
        echo "❌ Docker is not running. Please start Docker for integration tests."
        return 1
    fi
    echo "✅ Docker is running"
    return 0
}

# Function to run unit tests
run_unit_tests() {
    echo "🧪 Running unit tests..."
    mvn clean test -Dtest='!*IntegrationTest'
    echo "✅ Unit tests completed"
}

# Function to run integration tests
run_integration_tests() {
    echo "🔗 Running integration tests..."
    if check_docker; then
        mvn verify -Dtest='*IntegrationTest'
        echo "✅ Integration tests completed"
    else
        echo "⚠️  Skipping integration tests - Docker not available"
        return 1
    fi
}

# Function to generate coverage report
generate_coverage_report() {
    echo "📊 Generating code coverage report..."
    mvn jacoco:report
    
    if [ -f "target/site/jacoco/index.html" ]; then
        echo "✅ Coverage report generated: target/site/jacoco/index.html"
        
        # Extract coverage percentages from the report
        if command -v grep &> /dev/null && command -v sed &> /dev/null; then
            echo
            echo "📈 Coverage Summary:"
            if [ -f "target/site/jacoco/index.html" ]; then
                echo "   View detailed report: file://$(pwd)/target/site/jacoco/index.html"
            fi
        fi
    else
        echo "⚠️  Coverage report not found"
    fi
}

# Function to run all tests
run_all_tests() {
    echo "🚀 Running all tests..."
    mvn clean verify
    echo "✅ All tests completed"
}

# Function to run quick tests (unit tests only)
run_quick_tests() {
    echo "⚡ Running quick tests (unit tests only)..."
    run_unit_tests
}

# Function to validate coverage thresholds
validate_coverage() {
    echo "🎯 Validating coverage thresholds..."
    mvn jacoco:check
    if [ $? -eq 0 ]; then
        echo "✅ Coverage thresholds met (80% instruction, 70% branch)"
    else
        echo "❌ Coverage thresholds not met"
        return 1
    fi
}

# Display usage information
show_usage() {
    echo "Usage: $0 [OPTION]"
    echo "Run tests for the Kafka Spark Streaming project"
    echo
    echo "Options:"
    echo "  unit         Run unit tests only"
    echo "  integration  Run integration tests only (requires Docker)"
    echo "  coverage     Generate and display coverage report"
    echo "  validate     Validate coverage thresholds"
    echo "  quick        Run unit tests (alias for 'unit')"
    echo "  all          Run all tests (default)"
    echo "  help         Show this help message"
    echo
    echo "Examples:"
    echo "  $0           # Run all tests"
    echo "  $0 unit      # Run only unit tests"
    echo "  $0 coverage  # Generate coverage report"
}

# Parse command line arguments
case "${1:-all}" in
    "unit"|"quick")
        run_quick_tests
        ;;
    "integration")
        run_integration_tests
        ;;
    "coverage")
        run_unit_tests
        generate_coverage_report
        ;;
    "validate")
        run_unit_tests
        validate_coverage
        ;;
    "all")
        run_all_tests
        generate_coverage_report
        validate_coverage
        ;;
    "help"|"-h"|"--help")
        show_usage
        exit 0
        ;;
    *)
        echo "❌ Unknown option: $1"
        echo
        show_usage
        exit 1
        ;;
esac

echo
echo "🎉 Test execution completed!"