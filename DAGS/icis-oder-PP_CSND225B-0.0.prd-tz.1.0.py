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
                , WORKFLOW_NAME='PP_CSND225B',WORKFLOW_ID='f5976760b77a4348ad7a1a5cec8ac947', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-PP_CSND225B-0.0.prd-tz.1.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2024, 11, 1, 13, 0, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('f5976760b77a4348ad7a1a5cec8ac947')

    csnd225bJob_vol = []
    csnd225bJob_volMnt = []
    csnd225bJob_vol.append(getVolume('shared-volume','shared-volume'))
    csnd225bJob_volMnt.append(getVolumeMount('shared-volume','/app/order'))

    csnd225bJob_env = [getICISConfigMap('icis-oder-ppon-batch-configmap'), getICISConfigMap('icis-oder-ppon-batch-configmap2'), getICISSecret('icis-oder-ppon-batch-secret')]
    csnd225bJob_env.extend([getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret')])
    csnd225bJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    csnd225bJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '02b932b29655443e82be93137078d877',
        'volumes': csnd225bJob_vol,
        'volume_mounts': csnd225bJob_volMnt,
        'env_from':csnd225bJob_env,
        'task_id':'csnd225bJob',
        'image':'/icis/icis-oder-ppon-batch:0.4.1.49',
        'arguments':["--job.name=csnd225bJob",  "endTranDate=20240731" , "requestDate="+str(datetime.now())],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('f5976760b77a4348ad7a1a5cec8ac947')

    workflow = COMMON.getICISPipeline([
        authCheck,
        csnd225bJob,
        Complete
    ]) 

    # authCheck >> csnd225bJob >> Complete
    workflow








