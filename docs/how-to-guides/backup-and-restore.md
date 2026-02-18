# Backup and restore

## Prerequisites

Pick a backup target. The current setup assumes S3, but you can also use an
NFS-backed Restic repository. The NFS option is a good fit for a local NAS and
avoids running MinIO if you do not need an S3 API.

Create an S3 bucket to store backups. You can use AWS S3, Minio, or
any other S3-compatible provider.

- For AWS S3, your bucket URL might look something like this:
  `https://s3.amazonaws.com/my-homelab-backup`.
- For Minio, your bucket URL might look something like this:
  `https://my-s3-host.example.com/homelab-backup`.

Follow your provider's documentation to create a service account with the
following policy (replace `my-homelab-backup` with your actual bucket name):

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:PutObject",
        "s3:DeleteObject",
        "s3:ListBucket"
      ],
      "Resource": [
        "arn:aws:s3:::my-homelab-backup",
        "arn:aws:s3:::my-homelab-backup/*"
      ]
    }
  ]
}
```

Save the access key and secret key to a secure location, such as a password
manager. While you're at it, generate a new password for Restic encryption and
save it there as well.

### Alternative: NFS-backed Restic repository (NAS)

If you want to target a NAS over NFS, define an NFS-backed volume in the
system layer and mount it into the VolSync Restic data mover. Restic will use a
filesystem repository path on that mounted volume.

System layer expectations:

- An NFS export on the NAS dedicated to backups.
- A StorageClass and provisioner (preferred) or a static PersistentVolume that
  maps to that export.
- A PersistentVolumeClaim in each namespace that will run VolSync (PVCs are
  namespaced).

You will reference this PVC later via `moverVolumes` in the VolSync Restic
configuration and set `RESTIC_REPOSITORY` to a local path like
`/mnt/restic-repo/<namespace>/<pvc>`.

If you want the NFS definition to live in the system layer, consider:

- Defining an NFS StorageClass and provisioner as a system app.
- Keeping namespace-specific PVCs with the VolSync config in platform or
  application layers.

This repo includes a system app for the NFS provisioner at
`system/apps/nfs-provisioner`. Update `system/apps/nfs-provisioner/values.yaml`
with your NAS server IP and export path, then apply system apps as usual.

!!! example

    I use Minio for my homelab backups. Here's how I set it up:

    - Create a bucket named `homelab-backup`.
    - Create a service account under Identity -> Service Accounts -> Create
      Service Account:
        - Enable Restrict beyond user policy.
        - Paste the policy above.
        - Click Create and copy the access key and secret key
    - I also set up Minio replication to store backups in two locations: one in
      my house and one remotely.

## Add backup credentials to global secrets

Add the following to `external/terraform.tfvars`:

```hcl
extra_secrets = {
  restic-password = "xxxxxxxxxxxxxxxxxxxxxxxx"
  restic-s3-bucket = "https://s3.amazonaws.com/my-homelab-backup-xxxxxxxxxx"
  restic-s3-access-key = "xxxxxxxxxxxxxxxx"
  restic-s3-secret-key = "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"
}
```

Then apply the changes:

```sh
make external
```

You may want to back up the `external/terraform.tfvars` file to a secure location as well.

## Add backup configuration for volumes

!!! warning

    Do not run the backup command when building a new cluster where you intend
    to restore backups, as it may overwrite existing backup data. To restore
    data on a new cluster, refer to the [restore from
    backup](#restore-from-backup) section.

For now, you need to run a command to opt-in volumes until we have a better
GitOps solution:

```sh
make backup
```

This command will set up Restic repositories and back up the volumes configured
in `./Makefile`. You can adjust the list there to add or remove volumes from the
backup. You only need to run this command once, the backup configuration will
be stored in the cluster and run on a schedule.

## Restore from backup

The restore process is ad-hoc, you need to run a command to restore application volumes:

```sh
make restore
```

The command above will restore the latest backup of recommended volumes. Like
with backups, you can modify `./Makefile` to adjust the list of volumes you
want to restore.

## Gitea backups

This repo ships a nightly backup CronJob in `platform/apps/gitea` that
runs `gitea dump` into a dedicated `gitea-dump` PVC and then triggers VolSync
for that PVC. The dump PVC is mounted into the Gitea pod at `/dump`. This avoids
tying backups to Postgres replica counts and keeps the live Gitea data volume
out of the backup path.
The job removes any existing dump zips before creating a new timestamped dump,
so the PVC stays small while VolSync retains history.
For best results, set those ReplicationSources to manual-only triggers so the
CronJob is the only backup runner.

This relies on the relevant backup volumes having been created using the scripts/backup script:

```shell
./scripts/backup --action setup --namespace=gitea --pvc=gitea-dump --repo-type filesystem --schedule manual
```

Manual trigger:

```sh
kubectl -n gitea create job \
  --from=cronjob/gitea-backup \
  gitea-backup-manual-$(date +%Y%m%d%H%M%S)
```

Restore notes:

- Restore the `gitea-dump` PVC.
- Ensure Postgres is running and Gitea is ready.
- Run `gitea restore` from the latest dump zip stored in `/dump/`.

Example restore command:

```sh
kubectl -n gitea exec deploy/gitea -- \
  gitea restore --file /dump/gitea-dump-YYYYMMDDHHMMSS.zip
```

Example (filesystem repo):

```sh
./scripts/backup --action restore --namespace=gitea --pvc=gitea-dump --repo-type filesystem
```
