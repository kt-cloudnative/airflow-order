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
                , WORKFLOW_NAME='ET_CSNK102B',WORKFLOW_ID='07e381cab3af46a1a926ac9467c1eedf', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-ET_CSNK102B-0.4.prd-tz.0.1'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2024, 11, 1, 10, 17, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('07e381cab3af46a1a926ac9467c1eedf')

    csnk102bJob_vol = []
    csnk102bJob_volMnt = []
    csnk102bJob_vol.append(getVolume('shared-volume','shared-volume'))
    csnk102bJob_volMnt.append(getVolumeMount('shared-volume','/app/order'))

    csnk102bJob_env = [getICISConfigMap('icis-oder-etcterr-batch-configmap'), getICISConfigMap('icis-oder-etcterr-batch-configmap2'), getICISSecret('icis-oder-etcterr-batch-secret')]
    csnk102bJob_env.extend([getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret')])
    csnk102bJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    csnk102bJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '9a5a31f4a0354fe299883274696757b0',
        'volumes': csnk102bJob_vol,
        'volume_mounts': csnk102bJob_volMnt,
        'env_from':csnk102bJob_env,
        'task_id':'csnk102bJob',
        'image':'/icis/icis-oder-etcterr-batch:0.7.1.1',
        'arguments':["--job.name=csnk102bJob", "tranDate=20240731", "requestDate=${YYYYMMDDHHMISSSSS}"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('07e381cab3af46a1a926ac9467c1eedf')

    workflow = COMMON.getICISPipeline([
        authCheck,
        csnk102bJob,
        Complete
    ]) 

    # authCheck >> csnk102bJob >> Complete
    workflow








