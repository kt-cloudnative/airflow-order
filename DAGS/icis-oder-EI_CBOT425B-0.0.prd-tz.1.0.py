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
                , WORKFLOW_NAME='EI_CBOT425B',WORKFLOW_ID='52ea32271ea74a1ca6e5272d2eb071b9', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-EI_CBOT425B-0.0.prd-tz.1.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2025, 1, 10, 13, 40, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
    ,'max_active_runs':1
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('52ea32271ea74a1ca6e5272d2eb071b9')

    cbot425bJob_vol = []
    cbot425bJob_volMnt = []
    cbot425bJob_vol.append(getVolume('shared-volume','shared-volume'))
    cbot425bJob_volMnt.append(getVolumeMount('shared-volume','/app/order'))

    cbot425bJob_env = [getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret'), getICISConfigMap('icis-oder-configmap'), getICISSecret('icis-oder-secret')]
    cbot425bJob_env.extend([getICISConfigMap('icis-oder-entprinet-batch-mng-configmap'), getICISSecret('icis-oder-entprinet-batch-mng-secret'), getICISConfigMap('icis-oder-entprinet-batch-configmap'), getICISSecret('icis-oder-entprinet-batch-secret')])
    cbot425bJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    cbot425bJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : 'ca345ccd005741668c9fb74056b962c4',
        'volumes': cbot425bJob_vol,
        'volume_mounts': cbot425bJob_volMnt,
        'env_from':cbot425bJob_env,
        'task_id':'cbot425bJob',
        'image':'/icis/icis-oder-entprinet-batch:0.7.1.5',
        'arguments':["--job.name=cbot425bJob", "requestDate=${YYYYMMDDHHMISS}", "procDate=20241129", "progName=cbot425b"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('52ea32271ea74a1ca6e5272d2eb071b9')

    workflow = COMMON.getICISPipeline([
        authCheck,
        cbot425bJob,
        Complete
    ]) 

    # authCheck >> cbot425bJob >> Complete
    workflow








