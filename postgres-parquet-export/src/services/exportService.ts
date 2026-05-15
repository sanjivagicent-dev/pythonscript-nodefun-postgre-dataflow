import { exportTable } from './exportGeneric.js'

import {
  // Existing schemas
  wellSchema,
  productionSchema,
  invoiceSchema,

  // Red Bluff schemas
  rbAssetsSchema,
  rbPersonnelSchema,
  rbWellHistorySchema,
  rbFacilityHistorySchema,
  rbCompressorHistorySchema,
  rbSwdHistorySchema,
  rbTankBatteryHistorySchema,
  rbInvoicesSchema,
} from './parquetService.js'

export const exportData = async (prisma: any) => {
  await Promise.all([
    // ── EXISTING: Apex Permian ───────────────────────────────────────────

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

    // ── NEW: Red Bluff Resources ────────────────────────────────────────

    exportTable({
      prisma,
      tableName: 'rb_assets',
      schema: rbAssetsSchema,
    }),

    exportTable({
      prisma,
      tableName: 'rb_personnel',
      schema: rbPersonnelSchema,
    }),

    exportTable({
      prisma,
      tableName: 'rb_well_history',
      dateField: 'date',
      schema: rbWellHistorySchema,
    }),

    exportTable({
      prisma,
      tableName: 'rb_facility_history',
      dateField: 'date',
      schema: rbFacilityHistorySchema,
    }),

    exportTable({
      prisma,
      tableName: 'rb_compressor_history',
      dateField: 'date',
      schema: rbCompressorHistorySchema,
    }),

    exportTable({
      prisma,
      tableName: 'rb_swd_history',
      dateField: 'date',
      schema: rbSwdHistorySchema,
    }),

    exportTable({
      prisma,
      tableName: 'rb_tank_battery_history',
      dateField: 'date',
      schema: rbTankBatteryHistorySchema,
    }),

    exportTable({
      prisma,
      tableName: 'rb_invoices',
      dateField: 'invoice_date',
      schema: rbInvoicesSchema,
    }),
  ])
}