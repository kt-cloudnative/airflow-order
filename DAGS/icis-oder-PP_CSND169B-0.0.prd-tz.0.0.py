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
                , WORKFLOW_NAME='PP_CSND169B',WORKFLOW_ID='ae79786151f74f38b00afc5a6ce69384', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-PP_CSND169B-0.0.prd-tz.0.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2024, 11, 1, 12, 0, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('ae79786151f74f38b00afc5a6ce69384')

    csnd169bJob_vol = []
    csnd169bJob_volMnt = []
    csnd169bJob_vol.append(getVolume('shared-volume','shared-volume'))
    csnd169bJob_volMnt.append(getVolumeMount('shared-volume','/app/order'))

    csnd169bJob_env = [getICISConfigMap('icis-oder-ppon-batch-configmap'), getICISConfigMap('icis-oder-ppon-batch-configmap2'), getICISSecret('icis-oder-ppon-batch-secret')]
    csnd169bJob_env.extend([getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret')])
    csnd169bJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    csnd169bJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : 'ff361d1d362149f1b4bae6538bf7b86a',
        'volumes': csnd169bJob_vol,
        'volume_mounts': csnd169bJob_volMnt,
        'env_from':csnd169bJob_env,
        'task_id':'csnd169bJob',
        'image':'/icis/icis-oder-ppon-batch:0.4.1.19',
        'arguments':["--job.name=csnd169bJob", "endTranDate=${YYYYMMDD}", "pgmNm=csnd169b"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('ae79786151f74f38b00afc5a6ce69384')

    workflow = COMMON.getICISPipeline([
        authCheck,
        csnd169bJob,
        Complete
    ]) 

    # authCheck >> csnd169bJob >> Complete
    workflow








