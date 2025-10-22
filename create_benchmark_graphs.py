#!/usr/bin/env python3
"""
YCSB-cpp Benchmark Results Visualization
Generates comparison graphs from benchmark log files
"""

import os
import re
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from pathlib import Path

def parse_log_file(filepath):
    """Parse a YCSB log file to extract throughput information"""
    try:
        with open(filepath, 'r') as f:
            content = f.read()
        
        # Extract throughput from lines like "Run throughput(ops/sec): 1.44026e+06"
        throughput_match = re.search(r'Run throughput\(ops/sec\):\s*([\d.e+]+)', content)
        if throughput_match:
            return float(throughput_match.group(1))
        
        # If no run throughput found, try load throughput
        load_throughput_match = re.search(r'Load throughput\(ops/sec\):\s*([\d.e+]+)', content)
        if load_throughput_match:
            return float(load_throughput_match.group(1))
            
        return 0  # Failed or no data
    except Exception as e:
        print(f"Error parsing {filepath}: {e}")
        return 0

def collect_benchmark_data(results_dir):
    """Collect all benchmark data from log files"""
    databases = ['rocksdb', 'leveldb', 'lmdb', 'wiredtiger', 'leaves']
    workloads = ['workloada', 'workloadb', 'workloadc', 'workloadd', 'workloade', 'workloadf']
    
    data = []
    
    for db in databases:
        for workload in workloads:
            # Look for run phase results first, then load phase
            for phase in ['run', 'load']:
                pattern = f"{db}_{workload}_{phase}_*.log"
                log_files = list(Path(results_dir).glob(pattern))
                
                if log_files:
                    # Use the most recent file
                    log_file = sorted(log_files)[-1]
                    throughput = parse_log_file(log_file)
                    
                    if throughput > 0:
                        data.append({
                            'Database': db.upper(),
                            'Workload': workload.upper(),
                            'Phase': phase.capitalize(),
                            'Throughput': throughput,
                            'Throughput_MB': throughput / 1_000_000  # Convert to millions
                        })
                        break  # Use run phase if available, otherwise load phase
    
    return pd.DataFrame(data)

def create_comparison_graphs(df, output_dir):
    """Create multiple comparison graphs"""
    
    # Set up the plotting style
    plt.style.use('default')
    colors = ['#1f77b4', '#ff7f0e', '#2ca02c', '#d62728', '#9467bd', '#8c564b']
    
    # 1. Throughput by Database and Workload (Bar Chart)
    fig, ax = plt.subplots(figsize=(15, 8))
    
    # Pivot data for easier plotting
    pivot_df = df.pivot(index='Workload', columns='Database', values='Throughput_MB').fillna(0)
    
    # Create grouped bar chart
    x = np.arange(len(pivot_df.index))
    width = 0.2
    multiplier = 0
    
    for db in pivot_df.columns:
        offset = width * multiplier
        bars = ax.bar(x + offset, pivot_df[db], width, label=db, alpha=0.8)
        
        # Add value labels on bars
        for bar in bars:
            height = bar.get_height()
            if height > 0:
                ax.text(bar.get_x() + bar.get_width()/2., height + 0.1,
                       f'{height:.1f}M', ha='center', va='bottom', fontsize=8)
        
        multiplier += 1
    
    ax.set_xlabel('YCSB Workload', fontsize=12, fontweight='bold')
    ax.set_ylabel('Throughput (Million ops/sec)', fontsize=12, fontweight='bold')
    ax.set_title('YCSB-cpp Database Performance Comparison\nThroughput by Workload', 
                fontsize=14, fontweight='bold', pad=20)
    ax.set_xticks(x + width * 1.5)
    ax.set_xticklabels(pivot_df.index)
    ax.legend(title='Database', loc='upper right')
    ax.grid(True, alpha=0.3, axis='y')
    
    plt.tight_layout()
    plt.savefig(f"{output_dir}/throughput_comparison.png", dpi=300, bbox_inches='tight')
    plt.close()
    
    # 2. Database Performance Radar Chart
    fig, ax = plt.subplots(figsize=(10, 10), subplot_kw=dict(projection='polar'))
    
    # Prepare data for radar chart
    workloads = pivot_df.index.tolist()
    angles = np.linspace(0, 2 * np.pi, len(workloads), endpoint=False).tolist()
    angles += angles[:1]  # Complete the circle
    
    for i, db in enumerate(pivot_df.columns):
        values = pivot_df[db].tolist()
        if max(values) > 0:  # Only plot if database has data
            values += values[:1]  # Complete the circle
            ax.plot(angles, values, 'o-', linewidth=2, label=db, color=colors[i % len(colors)])
            ax.fill(angles, values, alpha=0.25, color=colors[i % len(colors)])
    
    ax.set_xticks(angles[:-1])
    ax.set_xticklabels(workloads)
    ax.set_ylim(0, max(pivot_df.max()) * 1.1)
    ax.set_title('Database Performance Radar\n(Million ops/sec)', 
                fontsize=14, fontweight='bold', pad=30)
    ax.legend(loc='upper right', bbox_to_anchor=(1.3, 1.0))
    ax.grid(True)
    
    plt.tight_layout()
    plt.savefig(f"{output_dir}/performance_radar.png", dpi=300, bbox_inches='tight')
    plt.close()
    
    # 3. Workload Characteristics Analysis
    fig, axes = plt.subplots(2, 3, figsize=(18, 12))
    fig.suptitle('YCSB Workload Performance Analysis', fontsize=16, fontweight='bold')
    
    workload_descriptions = {
        'WORKLOADA': 'Update Heavy\n(50% Read, 50% Update)',
        'WORKLOADB': 'Read Mostly\n(95% Read, 5% Update)',
        'WORKLOADC': 'Read Only\n(100% Read)',
        'WORKLOADD': 'Read Latest\n(95% Read, 5% Insert)',
        'WORKLOADE': 'Short Ranges\n(95% Scan, 5% Insert)',
        'WORKLOADF': 'Read-Modify-Write\n(50% Read, 50% RMW)'
    }
    
    for i, workload in enumerate(workloads):
        row = i // 3
        col = i % 3
        ax = axes[row, col]
        
        workload_data = df[df['Workload'] == workload]
        
        if not workload_data.empty:
            databases = workload_data['Database'].tolist()
            throughputs = workload_data['Throughput_MB'].tolist()
            
            bars = ax.bar(databases, throughputs, color=colors[:len(databases)], alpha=0.7)
            
            # Add value labels
            for bar in bars:
                height = bar.get_height()
                if height > 0:
                    ax.text(bar.get_x() + bar.get_width()/2., height + max(throughputs)*0.02,
                           f'{height:.1f}M', ha='center', va='bottom', fontweight='bold')
        
        ax.set_title(f'{workload}\n{workload_descriptions.get(workload, "")}', 
                    fontsize=11, fontweight='bold')
        ax.set_ylabel('Throughput (M ops/sec)')
        ax.tick_params(axis='x', rotation=45)
        ax.grid(True, alpha=0.3, axis='y')
    
    plt.tight_layout()
    plt.savefig(f"{output_dir}/workload_analysis.png", dpi=300, bbox_inches='tight')
    plt.close()
    
    # 4. Summary Table
    fig, ax = plt.subplots(figsize=(12, 8))
    ax.axis('tight')
    ax.axis('off')
    
    # Create summary statistics
    summary_stats = []
    for db in pivot_df.columns:
        db_data = pivot_df[db]
        if db_data.sum() > 0:
            summary_stats.append([
                db,
                f"{db_data.mean():.1f}M",
                f"{db_data.max():.1f}M",
                f"{db_data.min():.1f}M",
                f"{db_data.std():.1f}M",
                f"{(db_data > 0).sum()}/{len(db_data)}"
            ])
    
    table = ax.table(cellText=summary_stats,
                    colLabels=['Database', 'Avg Throughput', 'Max Throughput', 
                             'Min Throughput', 'Std Dev', 'Success Rate'],
                    cellLoc='center',
                    loc='center')
    
    table.auto_set_font_size(False)
    table.set_fontsize(12)
    table.scale(1.2, 1.5)
    
    # Style the table
    for i in range(len(summary_stats) + 1):
        for j in range(6):
            cell = table[(i, j)]
            if i == 0:  # Header row
                cell.set_facecolor('#40466e')
                cell.set_text_props(weight='bold', color='white')
            else:
                cell.set_facecolor('#f1f1f2')
    
    ax.set_title('Database Performance Summary Statistics', 
                fontsize=14, fontweight='bold', pad=20)
    
    plt.tight_layout()
    plt.savefig(f"{output_dir}/performance_summary.png", dpi=300, bbox_inches='tight')
    plt.close()
    
    return len(summary_stats)

def main():
    results_dir = "benchmark_results"
    output_dir = "benchmark_graphs"
    
    # Create output directory
    os.makedirs(output_dir, exist_ok=True)
    
    print("📊 Analyzing YCSB-cpp benchmark results...")
    
    # Collect data
    df = collect_benchmark_data(results_dir)
    
    if df.empty:
        print("❌ No benchmark data found!")
        return
    
    print(f"✅ Found {len(df)} benchmark results")
    print(f"📈 Databases tested: {df['Database'].unique()}")
    print(f"🔬 Workloads tested: {df['Workload'].unique()}")
    
    # Create graphs
    print("🎨 Creating comparison graphs...")
    num_successful = create_comparison_graphs(df, output_dir)
    
    print(f"\n🎉 Created 4 visualization files in '{output_dir}/':")
    print("   • throughput_comparison.png - Bar chart comparison")
    print("   • performance_radar.png - Radar chart analysis") 
    print("   • workload_analysis.png - Individual workload breakdown")
    print("   • performance_summary.png - Summary statistics table")
    
    # Print top performers
    print(f"\n🏆 Top Performers:")
    for workload in df['Workload'].unique():
        workload_data = df[df['Workload'] == workload].sort_values('Throughput', ascending=False)
        if not workload_data.empty:
            top = workload_data.iloc[0]
            print(f"   {workload}: {top['Database']} ({top['Throughput_MB']:.1f}M ops/sec)")

if __name__ == "__main__":
    main()