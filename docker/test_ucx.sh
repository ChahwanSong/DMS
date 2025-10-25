#!/bin/bash


# UCX 기반 Open MPI 테스트 스크립트
cat > hello_mpi.c <<'EOF'
#include <mpi.h>
#include <stdio.h>
#include <unistd.h>

int main(int argc, char** argv) {
    MPI_Init(&argc, &argv);
    int rank, size, len;
    char name[MPI_MAX_PROCESSOR_NAME];
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);
    MPI_Get_processor_name(name, &len);

    printf("Hello from rank %d/%d on %s\n", rank, size, name);

    MPI_Barrier(MPI_COMM_WORLD);
    MPI_Finalize();
    return 0;
}
EOF

# MPI 프로그램 컴파일:
mpicc -O2 hello_mpi.c -o hello_mpi

# UCX 기반 Open MPI로 MPI 프로그램 실행:
mpirun \
  --mca pml ucx \
  --mca pml_ucx_tls self,tcp \
  --mca pml_ucx_devices all \
  -x UCX_TLS=tcp,self \
  -x UCX_NET_DEVICES=all \
  -np 4 ./hello_mpi
  
# 현재 pml_ucx 관련 MCA 파라미터 보기:
ompi_info --param pml ucx