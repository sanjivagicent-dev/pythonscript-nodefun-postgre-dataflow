import dayjs from 'dayjs'

export const getNextChunk = (start: dayjs.Dayjs) => {
  return start.add(30, 'day')
}
