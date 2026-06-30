/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.paimon.iceberg;

import org.apache.paimon.Snapshot;
import org.apache.paimon.fs.Path;
import org.apache.paimon.iceberg.manifest.IcebergDataFileMeta;
import org.apache.paimon.iceberg.manifest.IcebergManifestEntry;
import org.apache.paimon.iceberg.manifest.IcebergManifestFile;
import org.apache.paimon.iceberg.manifest.IcebergManifestFileMeta;
import org.apache.paimon.iceberg.manifest.IcebergManifestList;
import org.apache.paimon.iceberg.metadata.IcebergDataField;
import org.apache.paimon.iceberg.metadata.IcebergMetadata;
import org.apache.paimon.iceberg.metadata.IcebergPartitionField;
import org.apache.paimon.iceberg.metadata.IcebergPartitionSpec;
import org.apache.paimon.iceberg.metadata.IcebergSchema;
import org.apache.paimon.iceberg.metadata.IcebergSnapshot;
import org.apache.paimon.iceberg.metadata.IcebergSnapshotSummary;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.manifest.FileKind;
import org.apache.paimon.manifest.ManifestCommittable;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.manifest.ManifestFile;
import org.apache.paimon.manifest.ManifestFileMeta;
import org.apache.paimon.manifest.ManifestList;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.CommitCallback;
import org.apache.paimon.table.sink.TagCallback;
import org.apache.paimon.types.BigIntType;
import org.apache.paimon.types.IntType;
import org.apache.paimon.utils.DataFilePathFactories;
import org.apache.paimon.utils.SnapshotManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * A {@link CommitCallback} that emits Iceberg metadata for Paimon changelog files produced by
 * {@code changelog-producer=lookup}. Creates a companion Iceberg table at {@code
 * <iceberg-root>/<table>_changelog/metadata/} with no data duplication — only manifest pointers to
 * the existing {@code changelog-*.parquet} files that Paimon already writes to GCS.
 *
 * <p>Enabled by {@code metadata.iceberg.changelog.storage} (alongside {@code
 * metadata.iceberg.storage}). The resulting Iceberg table exposes all changelog fields plus:
 *
 * <ul>
 *   <li>{@code _value_kind} (int): 0=INSERT, 1=UPDATE_BEFORE, 2=UPDATE_AFTER, 3=DELETE
 *   <li>{@code _sequence_number} (long): monotonically increasing within a checkpoint
 * </ul>
 *
 * <p>Each commit rebuilds the full Iceberg manifest from all currently-retained Paimon snapshots.
 * With default retention of 3–5 snapshots this is inexpensive.
 */
public class IcebergChangelogCommitCallback implements CommitCallback, TagCallback {

    private static final Logger LOG = LoggerFactory.getLogger(IcebergChangelogCommitCallback.class);

    private static final String VERSION_HINT_FILENAME = "version-hint.text";

    private final FileStoreTable table;
    private final IcebergPathFactory pathFactory;
    private final IcebergManifestFile icebergManifestFile;
    private final IcebergManifestList icebergManifestList;
    private final ManifestList paimonManifestList;
    private final ManifestFile paimonManifestFile;
    private final int formatVersion;

    public IcebergChangelogCommitCallback(FileStoreTable table, String commitUser) {
        this.table = table;

        // Changelog Iceberg metadata lives at <icebergDBPath>/<tableName>_changelog/metadata
        Path icebergDBPath = IcebergCommitCallback.catalogDatabasePath(table);
        Path changelogMetadataPath =
                new Path(
                        icebergDBPath,
                        String.format("%s_changelog/metadata", table.location().getName()));
        this.pathFactory = new IcebergPathFactory(changelogMetadataPath);

        this.icebergManifestFile = IcebergManifestFile.create(table, pathFactory);
        this.icebergManifestList = IcebergManifestList.create(table, pathFactory);

        this.paimonManifestList = table.store().manifestListFactory().create();
        this.paimonManifestFile = table.store().manifestFileFactory().create();

        this.formatVersion =
                table.coreOptions().toConfiguration().get(IcebergOptions.FORMAT_VERSION);
    }

    @Override
    public void close() throws Exception {}

    @Override
    public void call(Context context) {
        if (context.snapshot.changelogManifestList() == null) {
            return;
        }
        try {
            createChangelogMetadata(context.snapshot.id());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public void retry(ManifestCommittable committable) {
        // Retry is a best-effort operation for changelog metadata.
        // If the snapshot was committed, the changelog metadata should already exist.
        // If not, the next successful commit will trigger a full rebuild.
        LOG.debug("IcebergChangelogCommitCallback retry is a no-op");
    }

    // -----------------------------------------------------------------------------------------
    // Core metadata writer
    // -----------------------------------------------------------------------------------------

    private void createChangelogMetadata(long latestSnapshotId) throws IOException {
        Path targetPath = pathFactory.toMetadataPath(latestSnapshotId);
        if (table.fileIO().exists(targetPath)) {
            return;
        }

        SnapshotManager snapshotManager = table.snapshotManager();
        Long earliestId = snapshotManager.earliestSnapshotId();
        if (earliestId == null) {
            return;
        }

        // Schema: user columns from the latest Paimon table schema
        SchemaManager schemaManager =
                new SchemaManager(table.fileIO(), table.location(), table.coreOptions().branch());
        TableSchema tableSchema = schemaManager.latest().orElse(null);
        if (tableSchema == null) {
            return;
        }

        IcebergSchema baseSchema = IcebergSchema.create(tableSchema);
        IcebergSchema changelogSchema = buildChangelogSchema(baseSchema);
        List<IcebergPartitionField> partitionFields = buildPartitionFields(tableSchema, baseSchema);

        // Collect all live changelog file entries from all currently-retained Paimon snapshots
        DataFilePathFactories pathFactories =
                new DataFilePathFactories(table.store().pathFactory());
        List<IcebergManifestEntry> entries = new ArrayList<>();

        for (long snapshotId = earliestId; snapshotId <= latestSnapshotId; snapshotId++) {
            Snapshot snapshot;
            try {
                snapshot = snapshotManager.snapshot(snapshotId);
            } catch (Exception e) {
                LOG.debug("Snapshot {} no longer exists, skipping for changelog metadata",
                        snapshotId);
                continue;
            }
            if (snapshot.changelogManifestList() == null) {
                continue;
            }

            List<ManifestFileMeta> changelogManifestMetas =
                    paimonManifestList.readChangelogManifests(snapshot);
            for (ManifestFileMeta meta : changelogManifestMetas) {
                List<ManifestEntry> manifestEntries =
                        paimonManifestFile.read(meta.fileName(), meta.fileSize());
                for (ManifestEntry entry : manifestEntries) {
                    if (entry.kind() != FileKind.ADD) {
                        continue;
                    }
                    DataFileMeta fileMeta = entry.file();
                    String filePath =
                            pathFactories
                                    .get(entry.partition(), entry.bucket())
                                    .toPath(fileMeta)
                                    .toString();
                    // Use baseSchema for per-file column stats to avoid indexing issues
                    // with the extra _value_kind / _sequence_number fields.
                    IcebergDataFileMeta icebergFileMeta =
                            IcebergDataFileMeta.create(
                                    IcebergDataFileMeta.Content.DATA,
                                    filePath,
                                    "parquet",
                                    entry.partition(),
                                    fileMeta.rowCount(),
                                    fileMeta.fileSize(),
                                    baseSchema,
                                    fileMeta.valueStats(),
                                    fileMeta.valueStatsCols());
                    entries.add(
                            new IcebergManifestEntry(
                                    IcebergManifestEntry.Status.ADDED,
                                    snapshotId,
                                    snapshotId,
                                    snapshotId,
                                    icebergFileMeta));
                }
            }
        }

        // Write Iceberg manifest list
        String manifestListFileName;
        if (entries.isEmpty()) {
            manifestListFileName =
                    icebergManifestList.writeWithoutRolling(Collections.emptyList());
        } else {
            List<IcebergManifestFileMeta> manifestFileMetas =
                    icebergManifestFile.rollingWrite(entries.iterator(), latestSnapshotId);
            manifestListFileName = icebergManifestList.writeWithoutRolling(manifestFileMetas);
        }

        String tableUuid = getOrCreateTableUuid();

        IcebergSnapshot icebergSnapshot =
                new IcebergSnapshot(
                        latestSnapshotId,
                        latestSnapshotId,
                        null,
                        System.currentTimeMillis(),
                        IcebergSnapshotSummary.APPEND,
                        pathFactory.toManifestListPath(manifestListFileName).toString(),
                        baseSchema.schemaId(),
                        null,
                        null);

        IcebergMetadata metadata =
                new IcebergMetadata(
                        formatVersion,
                        tableUuid,
                        table.location().toString(),
                        latestSnapshotId,
                        changelogSchema.highestFieldId(),
                        Collections.singletonList(changelogSchema),
                        baseSchema.schemaId(),
                        Collections.singletonList(new IcebergPartitionSpec(partitionFields)),
                        partitionFields.stream()
                                .mapToInt(IcebergPartitionField::fieldId)
                                .max()
                                .orElse(IcebergPartitionField.FIRST_FIELD_ID - 1),
                        Collections.singletonList(icebergSnapshot),
                        (int) latestSnapshotId,
                        new HashMap<>());

        table.fileIO().tryToWriteAtomic(targetPath, metadata.toJson());
        table.fileIO()
                .overwriteFileUtf8(
                        new Path(pathFactory.metadataDirectory(), VERSION_HINT_FILENAME),
                        String.valueOf(latestSnapshotId));

        LOG.info(
                "Wrote changelog Iceberg metadata for snapshot {} with {} changelog files to {}",
                latestSnapshotId,
                entries.size(),
                targetPath);
    }

    // -----------------------------------------------------------------------------------------
    // Schema helpers
    // -----------------------------------------------------------------------------------------

    /**
     * Builds the Iceberg schema for the changelog companion table: all user columns from the
     * Paimon table schema, plus {@code _value_kind} (int) and {@code _sequence_number} (long)
     * appended at the end with fresh field IDs.
     */
    private IcebergSchema buildChangelogSchema(IcebergSchema baseSchema) {
        int highestFieldId = baseSchema.highestFieldId();
        List<IcebergDataField> fields = new ArrayList<>(baseSchema.fields());
        fields.add(
                new IcebergDataField(
                        highestFieldId + 1,
                        "_value_kind",
                        false,
                        "int",
                        new IntType(true),
                        "Paimon change type: 0=INSERT, 1=UPDATE_BEFORE, 2=UPDATE_AFTER, "
                                + "3=DELETE"));
        fields.add(
                new IcebergDataField(
                        highestFieldId + 2,
                        "_sequence_number",
                        false,
                        "long",
                        new BigIntType(true),
                        "Paimon sequence number, monotonically increasing within a checkpoint"));
        return new IcebergSchema(baseSchema.schemaId(), fields);
    }

    private List<IcebergPartitionField> buildPartitionFields(
            TableSchema tableSchema, IcebergSchema icebergSchema) {
        Map<String, IcebergDataField> fieldMap = new HashMap<>();
        for (IcebergDataField field : icebergSchema.fields()) {
            fieldMap.put(field.name(), field);
        }
        List<IcebergPartitionField> result = new ArrayList<>();
        int fieldId = IcebergPartitionField.FIRST_FIELD_ID;
        for (String partitionKey : tableSchema.partitionKeys()) {
            IcebergDataField field = fieldMap.get(partitionKey);
            if (field != null) {
                result.add(new IcebergPartitionField(field, fieldId));
                fieldId++;
            }
        }
        return result;
    }

    // -----------------------------------------------------------------------------------------
    // TableUUID helpers
    // -----------------------------------------------------------------------------------------

    /**
     * Returns the existing table UUID from the last written changelog metadata, or generates a
     * new UUID if no prior metadata exists. Keeps the UUID stable across snapshot commits.
     */
    private String getOrCreateTableUuid() throws IOException {
        Path versionHint =
                new Path(pathFactory.metadataDirectory(), VERSION_HINT_FILENAME);
        if (table.fileIO().exists(versionHint)) {
            try {
                String content = table.fileIO().readFileUtf8(versionHint).trim();
                long version = Long.parseLong(content);
                Path metadataPath = pathFactory.toMetadataPath(version);
                if (table.fileIO().exists(metadataPath)) {
                    return IcebergMetadata.fromPath(table.fileIO(), metadataPath).tableUuid();
                }
            } catch (Exception e) {
                LOG.debug("Could not read previous changelog metadata UUID, generating new one",
                        e);
            }
        }
        return UUID.randomUUID().toString();
    }

    // -----------------------------------------------------------------------------------------
    // TagCallback — changelog tables do not support tags
    // -----------------------------------------------------------------------------------------

    @Override
    public void notifyCreation(String tagName) {}

    @Override
    public void notifyDeletion(String tagName) {}
}
