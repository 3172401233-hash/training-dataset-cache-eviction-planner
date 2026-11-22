#!/usr/bin/env node
import { readFileSync } from 'node:fs';
import { planCache } from '../src/planner.js';

const file = process.argv[2];
if (!file) {
  console.error('用法: node bin/cli.js <trace.json>');
  process.exit(1);
}

const trace = JSON.parse(readFileSync(file, 'utf8'));
const plan = planCache(trace);
console.log(JSON.stringify(plan, null, 2));
