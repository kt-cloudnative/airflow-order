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
                , WORKFLOW_NAME='PP_CBOD201B',WORKFLOW_ID='3a34322f87ad432782806648dbcf6d23', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-PP_CBOD201B-0.0.prd-tz.2.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2025, 1, 10, 10, 25, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
    ,'max_active_runs': 16
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('3a34322f87ad432782806648dbcf6d23')

    cbod201bJob_vol = []
    cbod201bJob_volMnt = []
    cbod201bJob_vol.append(getVolume('t-order-ftp-pvc','t-order-ftp-pvc'))
    cbod201bJob_volMnt.append(getVolumeMount('t-order-ftp-pvc','/app/order'))

    cbod201bJob_env = [getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret'), getICISConfigMap('icis-oder-configmap'), getICISSecret('icis-oder-secret')]
    cbod201bJob_env.extend([getICISConfigMap('icis-oder-ppon-batch-mng-configmap'), getICISSecret('icis-oder-ppon-batch-mng-secret'), getICISConfigMap('icis-oder-ppon-batch-configmap'), getICISSecret('icis-oder-ppon-batch-secret')])
    cbod201bJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    cbod201bJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '81526b73d0b44683ae3c0b86360b0ef9',
        'volumes': cbod201bJob_vol,
        'volume_mounts': cbod201bJob_volMnt,
        'env_from':cbod201bJob_env,
        'task_id':'cbod201bJob',
        'image':'/icis/icis-oder-ppon-batch:0.7.1.7',
        'arguments':["--job.name=cbod201bJob", "endTranDate=20241129", "empNo=952794853", "requestDate="+str(datetime.now())],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('3a34322f87ad432782806648dbcf6d23')

    workflow = COMMON.getICISPipeline([
        authCheck,
        cbod201bJob,
        Complete
    ]) 

    # authCheck >> cbod201bJob >> Complete
    workflow








