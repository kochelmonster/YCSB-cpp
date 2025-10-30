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
    """Parse a YCSB log file to extract throughput and database size information"""
    try:
        with open(filepath, 'r') as f:
            content = f.read()
        
        # Extract throughput from lines like "Run throughput(ops/sec): 1.44026e+06"
        throughput_match = re.search(r'Run throughput\(ops/sec\):\s*([\d.e+]+)', content)
        throughput = 0
        if throughput_match:
            throughput = float(throughput_match.group(1))
        else:
            # If no run throughput found, try load throughput
            load_throughput_match = re.search(r'Load throughput\(ops/sec\):\s*([\d.e+]+)', content)
            if load_throughput_match:
                throughput = float(load_throughput_match.group(1))
        
        # Extract database size from lines like "Database size: 123M" or "Database size: 1.5G"
        size_match = re.search(r'Database size:\s*([\d.,]+)([KMG]?)', content)
        db_size_mb = 0
        if size_match:
            size_value = float(size_match.group(1).replace(',', '.'))
            size_unit = size_match.group(2)
            
            # Convert to MB
            if size_unit == 'G':
                db_size_mb = size_value * 1024
            elif size_unit == 'M':
                db_size_mb = size_value
            elif size_unit == 'K':
                db_size_mb = size_value / 1024
            else:
                db_size_mb = size_value / (1024 * 1024)  # Assume bytes
        
        return throughput, db_size_mb
            
    except Exception as e:
        print(f"Error parsing {filepath}: {e}")
        return 0, 0

def collect_benchmark_data(results_dir):
    """Collect data from all benchmark log files"""
    data = []
    
    # Get all log files
    log_files = list(Path(results_dir).glob("*_*.log"))
    
    for log_file in log_files:
        filename = log_file.name
        # Parse filename: db_workload_phase_timestamp.log
        # Handle workloads with underscores (e.g., workload_scan)
        parts = filename.replace('.log', '').split('_')
        
        if len(parts) < 4:  # Need at least db_workload_phase_timestamp
            continue
        
        db = parts[0]
        # Phase is either 'load' or 'run', which are the last operation-related parts before timestamp
        # Find the phase by looking for 'load' or 'run'
        phase_idx = -1
        for i, part in enumerate(parts):
            if part in ['load', 'run']:
                phase_idx = i
                break
        
        if phase_idx == -1:
            continue
        
        # Everything between db and phase is the workload name
        workload = '_'.join(parts[1:phase_idx])
        phase = parts[phase_idx]
        
        throughput, db_size_mb = parse_log_file(log_file)
        
        if throughput > 0:
            data.append({
                'Database': db,
                'Workload': workload,
                'Phase': phase,
                'Throughput': throughput,
                'Throughput_MB': throughput / 1_000_000,  # Convert to millions for better readability
                'DatabaseSize_MB': db_size_mb
            })
    
    return pd.DataFrame(data)

def create_comparison_graphs(df, output_dir):
    """Create various comparison graphs from the benchmark data"""
    
    # Ensure output_dir is a Path object
    output_dir = Path(output_dir)
    
    # Filter for 'run' phase only for consistency
    df_run = df[df['Phase'] == 'run']
    
    if df_run.empty:
        print("⚠️  No 'run' phase data found. Skipping graph generation.")
        return 0
    
    # Aggregate duplicates by taking the maximum value (most recent/best run)
    df_run = df_run.groupby(['Database', 'Workload', 'Phase']).agg({
        'Throughput': 'max',
        'Throughput_MB': 'max',
        'DatabaseSize_MB': 'max'
    }).reset_index()
    
    # 1. Throughput Comparison Bar Chart
    fig, ax = plt.subplots(figsize=(14, 8))
    
    # Create pivot table for grouped bar chart
    pivot_df = df_run.pivot(index='Workload', columns='Database', values='Throughput_MB').fillna(0)
    
    # Create grouped bar chart with better spacing
    x = np.arange(len(pivot_df.index))
    num_databases = len(pivot_df.columns)
    width = 0.8 / num_databases  # Adjust width based on number of databases
    multiplier = 0
    
    for db in pivot_df.columns:
        offset = width * multiplier
        bars = ax.bar(x + offset, pivot_df[db], width, label=db, alpha=0.8)
        
        # Add value labels on bars
        for bar in bars:
            height = bar.get_height()
            if height > 0:
                ax.text(bar.get_x() + bar.get_width()/2., height + 0.02,
                       f'{height:.2f}M', ha='center', va='bottom', fontsize=8)
        
        multiplier += 1
    
    ax.set_xlabel('YCSB Workload', fontsize=12, fontweight='bold')
    ax.set_ylabel('Throughput (Million ops/sec)', fontsize=12, fontweight='bold')
    ax.set_title('YCSB-cpp Database Performance Comparison\nThroughput by Workload', 
                fontsize=14, fontweight='bold', pad=20)
    ax.set_xticks(x + width * (num_databases - 1) / 2)
    ax.set_xticklabels(pivot_df.index)
    ax.legend(title='Database', loc='upper right')
    ax.grid(True, alpha=0.3, axis='y')
    
    plt.tight_layout()
    plt.savefig(f"{output_dir}/throughput_comparison.png", dpi=300, bbox_inches='tight')
    plt.close()
    
    # 2. Database Performance Radar Chart
    fig, ax = plt.subplots(figsize=(10, 10), subplot_kw=dict(projection='polar'))
    
    # Define color palette
    colors = plt.cm.Set3.colors
    
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
    # Dynamically calculate grid size based on number of workloads
    num_workloads = len(pivot_df.index)
    ncols = 3
    nrows = (num_workloads + ncols - 1) // ncols  # Ceiling division
    
    fig, axes = plt.subplots(nrows, ncols, figsize=(18, 6 * nrows))
    fig.suptitle('YCSB Workload Performance Analysis', fontsize=16, fontweight='bold')
    
    # Flatten axes for easier indexing
    if nrows == 1:
        axes = axes.reshape(1, -1)
    axes_flat = axes.flatten()
    
    workload_descriptions = {
        'workloada': 'Update Heavy\n(50% Read, 50% Update)',
        'workloadb': 'Read Mostly\n(95% Read, 5% Update)',
        'workloadc': 'Read Only\n(100% Read)',
        'workloadd': 'Read Latest\n(95% Read, 5% Insert)',
        'workload_scan': 'Realistic Scan\n(50% Read, 50% Scan 10-100)',
        'workload_scan10': 'Small Scan\n(50% Read, 50% Scan-10)',
        'workload_scan100': 'Large Scan\n(50% Read, 50% Scan-100)',
        'workloadf': 'Read-Modify-Write\n(50% Read, 50% RMW)'
    }
    
    for i, workload in enumerate(pivot_df.index):
        ax = axes_flat[i]
        
        workload_data = df_run[df_run['Workload'] == workload]
        
        if not workload_data.empty:
            databases = workload_data['Database'].tolist()
            throughputs = workload_data['Throughput_MB'].tolist()
            
            # Create bars with proper spacing
            x_pos = np.arange(len(databases))
            bars = ax.bar(x_pos, throughputs, color=colors[:len(databases)], alpha=0.7, width=0.6)
            
            # Add value labels
            for bar in bars:
                height = bar.get_height()
                if height > 0:
                    ax.text(bar.get_x() + bar.get_width()/2., height + max(throughputs)*0.02,
                           f'{height:.2f}M', ha='center', va='bottom', fontweight='bold')
            
            ax.set_xticks(x_pos)
            ax.set_xticklabels(databases, rotation=45, ha='right')
        
        ax.set_title(f'{workload}\n{workload_descriptions.get(workload, "")}', 
                    fontsize=11, fontweight='bold')
        ax.set_ylabel('Throughput (M ops/sec)')
        ax.grid(True, alpha=0.3, axis='y')
    
    # Hide any unused subplots
    for i in range(num_workloads, len(axes_flat)):
        axes_flat[i].axis('off')
    
    plt.tight_layout()
    plt.savefig(f"{output_dir}/workload_analysis.png", dpi=300, bbox_inches='tight')
    plt.close()
    
        # 4. Performance Summary Table
    fig, ax = plt.subplots(figsize=(14, 6))
    ax.axis('tight')
    ax.axis('off')
    
    # Create summary statistics
    summary_data = []
    for db in df['Database'].unique():
        db_data = df[df['Database'] == db]
        summary_data.append({
            'Database': db,
            'Avg Throughput (Mops/s)': f"{db_data['Throughput_MB'].mean():.2f}",
            'Max Throughput (Mops/s)': f"{db_data['Throughput_MB'].max():.2f}",
            'Min Throughput (Mops/s)': f"{db_data['Throughput_MB'].min():.2f}",
            'Workloads Tested': len(db_data['Workload'].unique())
        })
    
    summary_df = pd.DataFrame(summary_data)
    table = ax.table(cellText=summary_df.values, colLabels=summary_df.columns,
                     cellLoc='center', loc='center', bbox=[0, 0, 1, 1])
    table.auto_set_font_size(False)
    table.set_fontsize(9)
    table.scale(1, 2)
    
    # Style the header
    for i in range(len(summary_df.columns)):
        table[(0, i)].set_facecolor('#4CAF50')
        table[(0, i)].set_text_props(weight='bold', color='white')
    
    plt.title('Performance Summary Statistics', fontsize=16, weight='bold', pad=20)
    plt.savefig(output_dir / 'performance_summary.png', dpi=300, bbox_inches='tight')
    plt.close()
    
    # 5. Database Size Comparison
    size_df = df[df['Phase'] == 'run'].groupby(['Database', 'Workload'])['DatabaseSize_MB'].max().reset_index()
    
    if not size_df.empty and size_df['DatabaseSize_MB'].sum() > 0:
        fig, ax = plt.subplots(figsize=(14, 8))
        
        # Create pivot table for grouped bar chart
        size_pivot = size_df.pivot(index='Workload', columns='Database', values='DatabaseSize_MB')
        
        # Plot grouped bars
        size_pivot.plot(kind='bar', ax=ax, width=0.8)
        
        ax.set_xlabel('Workload', fontsize=12, weight='bold')
        ax.set_ylabel('Database Size (MB)', fontsize=12, weight='bold')
        ax.set_title('Database Storage Size Comparison', fontsize=16, weight='bold', pad=20)
        ax.legend(title='Database', bbox_to_anchor=(1.05, 1), loc='upper left')
        ax.grid(True, alpha=0.3)
        ax.set_axisbelow(True)
        
        # Add value labels on bars
        for container in ax.containers:
            ax.bar_label(container, fmt='%.1f', padding=3, fontsize=8)
        
        plt.xticks(rotation=0)
        plt.tight_layout()
        plt.savefig(output_dir / 'database_sizes.png', dpi=300, bbox_inches='tight')
        plt.close()
    
    print(f"   ✓ Graphs saved to {output_dir}/")
    
    return len(df['Database'].unique())

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
    
    print(f"\n🎉 Created 5 visualization files in '{output_dir}/':")
    print("   • throughput_comparison.png - Bar chart comparison")
    print("   • performance_radar.png - Radar chart analysis") 
    print("   • workload_analysis.png - Individual workload breakdown")
    print("   • performance_summary.png - Summary statistics table")
    print("   • database_sizes.png - Storage size comparison")
    
    # Print top performers (use aggregated data to match graphs)
    print(f"\n🏆 Top Performers:")
    df_run = df[df['Phase'] == 'run']
    if not df_run.empty:
        # Aggregate duplicates by taking the maximum value (same as graph logic)
        df_agg = df_run.groupby(['Database', 'Workload', 'Phase']).agg({
            'Throughput': 'max',
            'Throughput_MB': 'max',
            'DatabaseSize_MB': 'max'
        }).reset_index()
        
        for workload in sorted(df_agg['Workload'].unique()):
            workload_data = df_agg[df_agg['Workload'] == workload].sort_values('Throughput', ascending=False)
            if not workload_data.empty:
                top = workload_data.iloc[0]
                print(f"   {workload}: {top['Database']} ({top['Throughput_MB']:.1f}M ops/sec)")

if __name__ == "__main__":
    main()