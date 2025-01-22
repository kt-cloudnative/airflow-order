from datetime import datetime, timedelta
from kubernetes.client import models as k8s
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.subdag import SubDagOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.utils.helpers import chain, cross_downstream
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import (
    KubernetesPodOperator,
)
import pendulum
local_tz = pendulum.timezone("Asia/Seoul")
import sys
sys.path.append('/opt/bitnami/airflow/dags/git_sa-common')

from icis_common import *
COMMON = ICISCmmn(DOMAIN='oder',ENV='prd-tz', NAMESPACE='t-order'
                , WORKFLOW_NAME='ET_CBOG041B',WORKFLOW_ID='a4570b9634a248c08356950864ce2470', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-ET_CBOG041B-0.4.prd-tz.1.0'
    ,'schedule_interval':'None'
    ,'start_date': datetime(2025, 1, 10, 10, 46, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
    ,'max_active_runs': 16
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('a4570b9634a248c08356950864ce2470')

    cbog041bJob_vol = []
    cbog041bJob_volMnt = []
    cbog041bJob_vol.append(getVolume('t-order-ftp-pvc','t-order-ftp-pvc'))
    cbog041bJob_volMnt.append(getVolumeMount('t-order-ftp-pvc','/app/order'))

    cbog041bJob_env = [getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret'), getICISConfigMap('icis-oder-configmap'), getICISSecret('icis-oder-secret')]
    cbog041bJob_env.extend([getICISConfigMap('icis-oder-etcterr-batch-mng-configmap'), getICISSecret('icis-oder-etcterr-batch-mng-secret'), getICISConfigMap('icis-oder-etcterr-batch-configmap'), getICISSecret('icis-oder-etcterr-batch-secret')])
    cbog041bJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    cbog041bJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : 'cdc75626bce24920a17b2e90fc114b03',
        'volumes': cbog041bJob_vol,
        'volume_mounts': cbog041bJob_volMnt,
        'env_from':cbog041bJob_env,
        'task_id':'cbog041bJob',
        'image':'/icis/icis-oder-etcterr-batch:0.7.1.7',
        'arguments':["--job.name=cbog041bJob", "yyyyMm=${YYYYMM}", "inputFile=cdr202412.lst", "jobFlag=A", "stepFlag=Y", "requestDate=${YYYYMMDDHHMISSSSS}"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('a4570b9634a248c08356950864ce2470')

    workflow = COMMON.getICISPipeline([
        authCheck,
        cbog041bJob,
        Complete
    ]) 

    # authCheck >> cbog041bJob >> Complete
    workflow








