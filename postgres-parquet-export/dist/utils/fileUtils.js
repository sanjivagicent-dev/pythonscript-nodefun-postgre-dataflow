import fs from 'fs-extra';
export const ensureDir = async (dir) => {
    await fs.ensureDir(dir);
};
export const moveFile = async (src, dest) => {
    await fs.move(src, dest, { overwrite: true });
};
