
import json
import copy
import sys
import time
import os
import datetime
from typing import List, Dict, Any, Tuple

TIMING_FILE = "ivn_component_alignment_management_system_timings.json"

class OperationTimer:
    def __init__(self, operation_names):
        self.operation_names = operation_names
        self.timings = self._load_timings()
        self.start_times = {}
        self.elapsed = {}
        self.completed = 0
        self.total = len(operation_names)
        self.estimated_total = self._estimate_total_time()

    def _load_timings(self):
        if os.path.exists(TIMING_FILE):
            try:
                with open(TIMING_FILE, 'r') as f:
                    return json.load(f)
            except Exception:
                return {}
        return {}

    def _estimate_total_time(self):
        return sum(self.timings.get(op, 1.0) for op in self.operation_names)

    def start(self, op):
        self.start_times[op] = time.time()
        print(f"\nStarting operation: {op}")
        self._print_status(op, 0)

    def end(self, op):
        end_time = time.time()
        elapsed = end_time - self.start_times[op]
        self.elapsed[op] = elapsed
        self.completed += 1
        # Save timing for future runs
        self.timings[op] = elapsed
        with open(TIMING_FILE, 'w') as f:
            json.dump(self.timings, f, indent=2)
        self._print_status(op, elapsed, done=True)

    def _print_status(self, op, elapsed, done=False):
        est = self.timings.get(op, 1.0)
        remaining = max(0, est - elapsed)
        mins, secs = divmod(int(elapsed), 60)
        rmins, rsecs = divmod(int(remaining), 60)
        print(f"Operation: {op}")
        print(f"  Elapsed: {mins}m {secs}s")
        print(f"  Remaining: {rmins}m {rsecs}s")
        print(f"  Operations complete: {self.completed if done else self.completed}/{self.total}")
        print(f"  Operations remaining: {self.total - self.completed if done else self.total - self.completed + 1}")
        # Estimate total time left
        total_elapsed = sum(self.elapsed.get(o, 0) for o in self.operation_names[:self.completed])
        total_remaining = self.estimated_total - total_elapsed
        tmins, tsecs = divmod(int(total_remaining), 60)
        print(f"  Estimated time to complete all: {tmins}m {tsecs}s\n")

class ReferentialAlignmentSystem:
    def __init__(self, data: Dict[str, Any]):
        self.sources = set(data.get('sources', []))
        self.components = data.get('components', [])
        self.alignments = data.get('alignments', [])
        self.component_map = {}
        self.errors = []
        self._index_components()

    def _index_components(self):
        for comp in self.components:
            cid = comp.get('id')
            source = comp.get('source')
            if cid in self.component_map:
                self.errors.append({
                    'type': 'duplicate_component',
                    'component': cid,
                    'location': f'Component ID {cid} appears multiple times.'
                })
            self.component_map[cid] = source

    def validate(self) -> bool:
        # Validate sources
        for comp in self.components:
            source = comp.get('source')
            cid = comp.get('id')
            if source not in self.sources:
                self.errors.append({
                    'type': 'missing_source',
                    'component': cid,
                    'location': f'Component {cid} references missing source {source}.'
                })
        # Validate components in alignments
        seen_alignments = set()
        for align in self.alignments:
            from_id = align.get('from')
            to_id = align.get('to')
            key = (from_id, to_id)
            # Both components must exist
            if from_id not in self.component_map:
                self.errors.append({
                    'type': 'missing_component',
                    'alignment': key,
                    'location': f'Alignment from {from_id} to {to_id}: missing from-component.'
                })
            if to_id not in self.component_map:
                self.errors.append({
                    'type': 'missing_component',
                    'alignment': key,
                    'location': f'Alignment from {from_id} to {to_id}: missing to-component.'
                })
            # No self-alignment
            if from_id == to_id:
                self.errors.append({
                    'type': 'reflexive_alignment',
                    'alignment': key,
                    'location': f'Alignment from {from_id} to {to_id} is reflexive.'
                })
            # No same-source alignment
            if (from_id in self.component_map and to_id in self.component_map and
                self.component_map[from_id] == self.component_map[to_id]):
                self.errors.append({
                    'type': 'same_source_alignment',
                    'alignment': key,
                    'location': f'Alignment from {from_id} to {to_id}: both components from source {self.component_map[from_id]}.'
                })
            # Unique and directional
            if key in seen_alignments:
                self.errors.append({
                    'type': 'duplicate_alignment',
                    'alignment': key,
                    'location': f'Duplicate alignment from {from_id} to {to_id}.'
                })
            seen_alignments.add(key)
        return not self.errors

    def error_report(self) -> Dict[str, Any]:
        return {
            'status': 'error',
            'errors': self.errors
        }

    def apply_updates(self) -> Dict[str, Any]:
        # Transactional: deep copy and only commit if all valid
        new_state = {
            'sources': list(self.sources),
            'components': copy.deepcopy(self.components),
            'alignments': copy.deepcopy(self.alignments)
        }
        return {
            'status': 'success',
            'summary': {
                'sources_count': len(new_state['sources']),
                'components_count': len(new_state['components']),
                'alignments_count': len(new_state['alignments']),
                'sources': new_state['sources'],
                'components': new_state['components'],
                'alignments': new_state['alignments']
            }
        }


def main(input_json: str, timer: OperationTimer) -> Dict[str, Any]:
    timer.start('Input Parsing')
    data = json.loads(input_json)
    timer.end('Input Parsing')

    timer.start('Validation')
    system = ReferentialAlignmentSystem(data)
    valid = system.validate()
    timer.end('Validation')

    if not valid:
        timer.start('Error Reporting')
        result = system.error_report()
        timer.end('Error Reporting')
    else:
        timer.start('Transactional Update')
        result = system.apply_updates()
        timer.end('Transactional Update')

    # Include progress data in output
    progress = {
        'operations_completed': timer.completed,
        'total_operations': timer.total,
        'timings': timer.timings,
        'estimated_total_time': timer.estimated_total
    }
    full_output = {
        'result': result,
        'progress': progress
    }
    return full_output

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Run the Referential Alignment System")
    parser.add_argument('input_file', nargs='?', help='Path to input JSON file (optional, reads from stdin if not provided)')
    args = parser.parse_args()

    operation_names = [
        'Input Parsing',
        'Validation',
        'Error Reporting',
        'Transactional Update'
    ]
    timer = OperationTimer(operation_names)

    if args.input_file:
        with open(args.input_file, 'r') as f:
            input_data = f.read()
    else:
        input_data = sys.stdin.read()

    result = main(input_data, timer)
    
    # Save output to file with timestamp
    timestamp = datetime.datetime.now().strftime("%Y%m%d%H%M")
    output_filename = f"output_{timestamp}.json"
    output_json = json.dumps(result, indent=2)
    with open(output_filename, 'w') as f:
        f.write(output_json)
    full_output_path = os.path.abspath(output_filename)
    print(f"Output saved to {full_output_path}")
    
    print(output_json)
    print(f"Output file: {full_output_path}")
