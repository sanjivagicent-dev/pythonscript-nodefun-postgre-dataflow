import { Client } from 'pg';
import { config } from '../config/env.js';
export const createClient = () => {
    return new Client(config.db);
};
