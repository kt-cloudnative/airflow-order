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
                , WORKFLOW_NAME='BI_CSAB808B',WORKFLOW_ID='0cc0cda64f8b4ce1aee3c01784044df9', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-BI_CSAB808B-0.0.prd-tz.0.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2024, 8, 17, 13, 15, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
    ,'max_active_runs': 16
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('0cc0cda64f8b4ce1aee3c01784044df9')

    csab808bJob_vol = []
    csab808bJob_volMnt = []
    csab808bJob_env = [getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret'), getICISConfigMap('icis-oder-configmap'), getICISSecret('icis-oder-secret')]
    csab808bJob_env.extend([getICISConfigMap('icis-oder-baseinfo-batch-mng-configmap'), getICISSecret('icis-oder-baseinfo-batch-mng-secret'), getICISConfigMap('icis-oder-baseinfo-batch-configmap'), getICISSecret('icis-oder-baseinfo-batch-secret')])
    csab808bJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    csab808bJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '6e11dcd6ba554bea86690d1f42a8c0d1',
        'volumes': csab808bJob_vol,
        'volume_mounts': csab808bJob_volMnt,
        'env_from':csab808bJob_env,
        'task_id':'csab808bJob',
        'image':'/icis/icis-oder-baseinfo-batch:0.7.1.4',
        'arguments':["--job.name=csab808bJob", "workDate=20240720", "requestDate=${YYYYMMDDHHMISSSSS}"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('0cc0cda64f8b4ce1aee3c01784044df9')

    workflow = COMMON.getICISPipeline([
        authCheck,
        csab808bJob,
        Complete
    ]) 

    # authCheck >> csab808bJob >> Complete
    workflow








