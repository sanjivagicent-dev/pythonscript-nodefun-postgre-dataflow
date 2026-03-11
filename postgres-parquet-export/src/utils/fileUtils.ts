import fs from "fs-extra"

export const ensureDir = async (dir: string) => {
  await fs.ensureDir(dir)
}

export const moveFile = async (src: string, dest: string) => {
  await fs.move(src, dest, { overwrite: true })
}