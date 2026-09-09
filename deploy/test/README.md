# Integration-test dependencies

The integration suite needs a real Postgres and a real S3 API. Two
implementations of one contract are only worth having if both are exercised;
a mock would test our idea of Postgres rather than Postgres.

`docker-compose.test.yml` at the repository root is the default way to get
them. These manifests are the alternative for a machine with no local
container runtime, or for sharing one set of dependencies across a team.

## Bring them up

```sh
kubectl apply -f deploy/test/namespace.yaml
kubectl apply -f deploy/test/
kubectl -n dhole-test rollout status deploy/postgres deploy/minio
```

The first rollout is slow — it pulls two images onto a node that has never
seen them.

## Reach them

Both services are cluster-internal. Forward them to the same ports
`docker-compose.test.yml` uses, so the test environment is identical either
way:

```sh
kubectl -n dhole-test port-forward svc/postgres 55432:5432 &
kubectl -n dhole-test port-forward svc/minio    59000:9000 &
```

Then:

```sh
export DHOLE_TEST_POSTGRES_DSN='postgres://dhole:dholetestsecret@localhost:55432/dhole?sslmode=disable'
export DHOLE_TEST_S3_ENDPOINT='http://localhost:59000'
export DHOLE_TEST_S3_ACCESS_KEY=dholetest
export DHOLE_TEST_S3_SECRET_KEY=dholetestsecret
make test-integration
```

A test that cannot reach its dependency skips with a reason. It never passes
quietly — an integration test that silently degrades to nothing is worse than
no integration test, because it reports green.

## Tear them down

```sh
kubectl delete namespace dhole-test
```

## On the credentials in these files

They are literal, committed, test-only, and deliberately identical to the
defaults in `docker-compose.test.yml`, so `make test-integration` needs no
extra environment either way. The services hold nothing but
what a test run just created, they are reachable only from inside the cluster
or through an explicit port-forward, and every deployment here uses `emptyDir`
— restarting a pod discards its data. Do not copy this pattern to anything
that stores something real.
