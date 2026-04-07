import { exportTable } from './exportGeneric.js'
import {
  wellSchema,
  productionSchema,
  invoiceSchema,
} from './parquetService.js'

export const exportData = async (prisma: any) => {
  await Promise.all([
    exportTable({
      prisma,
      tableName: 'well_data',
      dateField: 'date',
      schema: wellSchema,
    }),
    exportTable({
      prisma,
      tableName: 'production_history',
      dateField: 'date',
      schema: productionSchema,
    }),
    exportTable({
      prisma,
      tableName: 'invoices',
      dateField: 'invoice_date',
      schema: invoiceSchema,
    }),
  ])
}